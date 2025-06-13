use std::sync::Arc;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Mutex,
};

const REQUEST_MAGIC: u8 = 0x80;
const RESPONSE_MAGIC: u8 = 0x81;

pub struct MemcacheClient {
    stream: Arc<Mutex<TcpStream>>,
}

impl MemcacheClient {
    pub async fn connect(addr: &str) -> Self {
        let stream = TcpStream::connect(addr)
            .await
            .expect("Failed to connect to Memcached");
        stream.set_nodelay(true).unwrap();
        Self {
            stream: Arc::new(Mutex::new(stream)),
        }
    }

    #[inline(always)]
    pub async fn set(
        &self,
        key: &str,
        value: &[u8],
        flags: u32,
        exptime: u32,
    ) -> Result<(), String> {
        let mut buf = Vec::with_capacity(128);
        encode_set_into(&mut buf, key, value, flags, exptime);
        let mut stream = self.stream.lock().await;
        stream
            .write_all(&buf)
            .await
            .map_err(|e| format!("Write error: {e}"))?;
        let mut header = [0u8; 24];
        stream
            .read_exact(&mut header)
            .await
            .map_err(|e| format!("Read error: {e}"))?;
        if header[0] != RESPONSE_MAGIC {
            return Err("Invalid response magic".to_string());
        }
        let status = u16::from_be_bytes([header[6], header[7]]);
        let total_body_len =
            u32::from_be_bytes([header[8], header[9], header[10], header[11]]) as usize;
        if total_body_len > 0 {
            let mut body = vec![0u8; total_body_len];
            stream
                .read_exact(&mut body)
                .await
                .map_err(|e| format!("Read error: {e}"))?;
        }
        if status == 0 {
            Ok(())
        } else {
            Err(format!("SET error status: {status}"))
        }
    }

    /// Set a key without waiting for a reply (hot path, fire-and-forget).
    #[inline(always)]
    pub async fn set_quietly(&self, key: &str, value: &[u8]) -> Result<(), String> {
        let mut buf = Vec::with_capacity(128);
        encode_set_quietly_into(&mut buf, key, value);
        let mut stream = self.stream.lock().await;
        stream
            .write_all(&buf)
            .await
            .map_err(|e| format!("Write error: {e}"))?;
        Ok(())
    }

    #[inline(always)]
    pub async fn set_multi<'a, I>(&self, items: I, flags: u32, exptime: u32) -> Result<(), String>
    where
        I: IntoIterator<Item = (&'a str, &'a [u8])>,
    {
        let mut stream = self.stream.lock().await;
        let mut batch_buf = Vec::with_capacity(4096);
        let mut count = 0;
        for (key, value) in items.into_iter() {
            encode_set_into(&mut batch_buf, key, value, flags, exptime);
            count += 1; // Increment for each item
        }
        if !batch_buf.is_empty() {
            stream
                .write_all(&batch_buf)
                .await
                .map_err(|e| format!("Write error: {e}"))?;
            // Each SET expects a response
            for _ in 0..count {
                let mut header = [0u8; 24];
                stream
                    .read_exact(&mut header)
                    .await
                    .map_err(|e| format!("Read error: {e}"))?;
                if header[0] != RESPONSE_MAGIC {
                    return Err("Invalid response magic".to_string());
                }
                let status = u16::from_be_bytes([header[6], header[7]]);
                let total_body_len =
                    u32::from_be_bytes([header[8], header[9], header[10], header[11]]) as usize;
                if total_body_len > 0 {
                    let mut body = vec![0u8; total_body_len];
                    stream
                        .read_exact(&mut body)
                        .await
                        .map_err(|e| format!("Read error: {e}"))?;
                }
                if status != 0 {
                    return Err(format!("SET error status: {status}"));
                }
            }
        }
        Ok(())
    }

    /// Batch set using SETQ for all but the last key, and SET for the last key (waits for one response).
    #[inline(always)]
    pub async fn set_multi_quietly<'a, I>(&self, items: I) -> Result<(), String>
    where
        I: IntoIterator<Item = (&'a str, &'a [u8])>,
    {
        let mut stream = self.stream.lock().await;
        let mut batch_buf = Vec::with_capacity(4096);
        let mut iter = items.into_iter().peekable();

        if iter.peek().is_none() {
            return Ok(());
        }

        while let Some((key, value)) = iter.next() {
            if iter.peek().is_some() {
                encode_set_quietly_into(&mut batch_buf, key, value);
            } else {
                encode_set_into(&mut batch_buf, key, value, 0, 0);
            }
        }

        stream
            .write_all(&batch_buf)
            .await
            .map_err(|e| format!("Write error: {e}"))?;

        // Only read one response (for the last SET)
        let mut header = [0u8; 24];
        stream
            .read_exact(&mut header)
            .await
            .map_err(|e| format!("Read error: {e}"))?;
        if header[0] != RESPONSE_MAGIC {
            return Err("Invalid response magic".to_string());
        }
        let status = u16::from_be_bytes([header[6], header[7]]);
        if status != 0 {
            return Err(format!("SET error status: {status}"));
        }
        Ok(())
    }

    #[inline(always)]
    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        let req = encode_get(key);
        let mut stream = self.stream.lock().await;
        stream
            .write_all(&req)
            .await
            .map_err(|e| format!("Write error: {e}"))?;
        let mut header = [0u8; 24];
        stream
            .read_exact(&mut header)
            .await
            .map_err(|e| format!("Read error: {e}"))?;
        if header[0] != RESPONSE_MAGIC {
            return Err("Invalid response magic".to_string());
        }
        let status = u16::from_be_bytes([header[6], header[7]]);
        let total_body_len =
            u32::from_be_bytes([header[8], header[9], header[10], header[11]]) as usize;
        let key_len = u16::from_be_bytes([header[2], header[3]]) as usize;
        let extras_len = header[4] as usize;
        let mut body = vec![0u8; total_body_len];
        if total_body_len > 0 {
            stream
                .read_exact(&mut body)
                .await
                .map_err(|e| format!("Read error: {e}"))?;
        }
        if status == 0 {
            let value = body[extras_len + key_len..].to_vec();
            Ok(Some(value))
        } else if status == 1 {
            Ok(None)
        } else {
            Err(format!("GET error status: {status}"))
        }
    }

    #[inline(always)]
    pub async fn get_multi<'a, I>(&self, keys: I) -> Result<Vec<(String, Option<Vec<u8>>)>, String>
    where
        I: IntoIterator<Item = &'a str>,
    {
        let mut stream = self.stream.lock().await;
        let mut batch_buf = Vec::with_capacity(4096);
        let mut key_vec = Vec::new();
        for key in keys.into_iter() {
            encode_get_into(&mut batch_buf, key);
            key_vec.push(key.to_string());
        }
        if !batch_buf.is_empty() {
            stream
                .write_all(&batch_buf)
                .await
                .map_err(|e| format!("Write error: {e}"))?;
        }
        let mut results = Vec::new();
        for key in key_vec {
            let mut header = [0u8; 24];
            stream
                .read_exact(&mut header)
                .await
                .map_err(|e| format!("Read error: {e}"))?;
            if header[0] != RESPONSE_MAGIC {
                return Err("Invalid response magic".to_string());
            }
            let status = u16::from_be_bytes([header[6], header[7]]);
            let total_body_len =
                u32::from_be_bytes([header[8], header[9], header[10], header[11]]) as usize;
            let key_len = u16::from_be_bytes([header[2], header[3]]) as usize;
            let extras_len = header[4] as usize;
            let mut body = vec![0u8; total_body_len];
            if total_body_len > 0 {
                stream
                    .read_exact(&mut body)
                    .await
                    .map_err(|e| format!("Read error: {e}"))?;
            }
            if status == 0 {
                let value = body[extras_len + key_len..].to_vec();
                results.push((key, Some(value)));
            } else if status == 1 {
                results.push((key, None));
            } else {
                return Err(format!("GET error status: {status}"));
            }
        }
        Ok(results)
    }

    /// Delete a single key, waits for response.
    #[inline(always)]
    pub async fn delete(&self, key: &str) -> Result<(), String> {
        let mut buf = Vec::with_capacity(64);
        encode_delete_into(&mut buf, key);
        let mut stream = self.stream.lock().await;
        stream
            .write_all(&buf)
            .await
            .map_err(|e| format!("Write error: {e}"))?;
        let mut header = [0u8; 24];
        stream
            .read_exact(&mut header)
            .await
            .map_err(|e| format!("Read error: {e}"))?;
        if header[0] != RESPONSE_MAGIC {
            return Err("Invalid response magic".to_string());
        }
        let status = u16::from_be_bytes([header[6], header[7]]);
        if status == 0 {
            Ok(())
        } else if status == 1 {
            // Not found is not an error for delete
            Ok(())
        } else {
            Err(format!("DELETE error status: {status}"))
        }
    }

    /// Delete a single key, quietly (no response).
    #[inline(always)]
    pub async fn delete_quietly(&self, key: &str) -> Result<(), String> {
        let mut buf = Vec::with_capacity(64);
        encode_delete_quiet_into(&mut buf, key);
        let mut stream = self.stream.lock().await;
        stream
            .write_all(&buf)
            .await
            .map_err(|e| format!("Write error: {e}"))?;
        Ok(())
    }

    /// Delete multiple keys, waits for response for each.
    #[inline(always)]
    pub async fn delete_multi<'a, I>(&self, keys: I) -> Result<(), String>
    where
        I: IntoIterator<Item = &'a str>,
    {
        let mut stream = self.stream.lock().await;
        let mut batch_buf = Vec::with_capacity(4096);
        let mut count = 0;
        for key in keys.into_iter() {
            encode_delete_into(&mut batch_buf, key);
            count += 1;
        }
        if !batch_buf.is_empty() {
            stream
                .write_all(&batch_buf)
                .await
                .map_err(|e| format!("Write error: {e}"))?;
            for _ in 0..count {
                let mut header = [0u8; 24];
                stream
                    .read_exact(&mut header)
                    .await
                    .map_err(|e| format!("Read error: {e}"))?;
                if header[0] != RESPONSE_MAGIC {
                    return Err("Invalid response magic".to_string());
                }
                let status = u16::from_be_bytes([header[6], header[7]]);
                if status != 0 && status != 1 {
                    return Err(format!("DELETE error status: {status}"));
                }
            }
        }
        Ok(())
    }

    /// Delete multiple keys, quietly (all but last as DELQ, last as DEL).
    #[inline(always)]
    pub async fn delete_multi_quietly<'a, I>(&self, keys: I) -> Result<(), String>
    where
        I: IntoIterator<Item = &'a str>,
    {
        let mut stream = self.stream.lock().await;
        let mut batch_buf = Vec::with_capacity(4096);
        let mut iter = keys.into_iter().peekable();

        if iter.peek().is_none() {
            return Ok(());
        }

        while let Some(key) = iter.next() {
            if iter.peek().is_some() {
                encode_delete_quiet_into(&mut batch_buf, key);
            } else {
                encode_delete_into(&mut batch_buf, key);
            }
        }

        stream
            .write_all(&batch_buf)
            .await
            .map_err(|e| format!("Write error: {e}"))?;

        // Only read one response (for the last DEL)
        let mut header = [0u8; 24];
        stream
            .read_exact(&mut header)
            .await
            .map_err(|e| format!("Read error: {e}"))?;
        if header[0] != RESPONSE_MAGIC {
            return Err("Invalid response magic".to_string());
        }
        let status = u16::from_be_bytes([header[6], header[7]]);
        if status != 0 && status != 1 {
            return Err(format!("DELETE error status: {status}"));
        }
        Ok(())
    }

    /// Increment a key by `delta`. Returns the new value or None if not found.
    #[inline(always)]
    pub async fn incr(
        &self,
        key: &str,
        delta: u64,
        initial: u64,
        exptime: u32,
    ) -> Result<Option<u64>, String> {
        let mut buf = Vec::with_capacity(64);
        encode_incr_decr_into(&mut buf, key, delta, initial, exptime, true);
        let mut stream = self.stream.lock().await;
        stream
            .write_all(&buf)
            .await
            .map_err(|e| format!("Write error: {e}"))?;
        let mut header = [0u8; 24];
        stream
            .read_exact(&mut header)
            .await
            .map_err(|e| format!("Read error: {e}"))?;
        if header[0] != RESPONSE_MAGIC {
            return Err("Invalid response magic".to_string());
        }
        let status = u16::from_be_bytes([header[6], header[7]]);
        let total_body_len =
            u32::from_be_bytes([header[8], header[9], header[10], header[11]]) as usize;
        let mut body = vec![0u8; total_body_len];
        if total_body_len > 0 {
            stream
                .read_exact(&mut body)
                .await
                .map_err(|e| format!("Read error: {e}"))?;
        }
        if status == 0 {
            // Body is 8 bytes: new value
            let val = u64::from_be_bytes(body[..8].try_into().unwrap());
            Ok(Some(val))
        } else if status == 1 {
            Ok(None)
        } else {
            Err(format!("INCR error status: {status}"))
        }
    }

    /// Decrement a key by `delta`. Returns the new value or None if not found.
    #[inline(always)]
    pub async fn decr(
        &self,
        key: &str,
        delta: u64,
        initial: u64,
        exptime: u32,
    ) -> Result<Option<u64>, String> {
        let mut buf = Vec::with_capacity(64);
        encode_incr_decr_into(&mut buf, key, delta, initial, exptime, false);
        let mut stream = self.stream.lock().await;
        stream
            .write_all(&buf)
            .await
            .map_err(|e| format!("Write error: {e}"))?;
        let mut header = [0u8; 24];
        stream
            .read_exact(&mut header)
            .await
            .map_err(|e| format!("Read error: {e}"))?;
        if header[0] != RESPONSE_MAGIC {
            return Err("Invalid response magic".to_string());
        }
        let status = u16::from_be_bytes([header[6], header[7]]);
        let total_body_len =
            u32::from_be_bytes([header[8], header[9], header[10], header[11]]) as usize;
        let mut body = vec![0u8; total_body_len];
        if total_body_len > 0 {
            stream
                .read_exact(&mut body)
                .await
                .map_err(|e| format!("Read error: {e}"))?;
        }
        if status == 0 {
            let val = u64::from_be_bytes(body[..8].try_into().unwrap());
            Ok(Some(val))
        } else if status == 1 {
            Ok(None)
        } else {
            Err(format!("DECR error status: {status}"))
        }
    }

    /// Send a NOOP command (useful for pipelining).
    #[inline(always)]
    pub async fn noop(&self) -> Result<(), String> {
        let mut buf = Vec::with_capacity(24);
        encode_noop_into(&mut buf);
        let mut stream = self.stream.lock().await;
        stream
            .write_all(&buf)
            .await
            .map_err(|e| format!("Write error: {e}"))?;
        let mut header = [0u8; 24];
        stream
            .read_exact(&mut header)
            .await
            .map_err(|e| format!("Read error: {e}"))?;
        if header[0] != RESPONSE_MAGIC {
            return Err("Invalid response magic".to_string());
        }
        let status = u16::from_be_bytes([header[6], header[7]]);
        if status == 0 {
            Ok(())
        } else {
            Err(format!("NOOP error status: {status}"))
        }
    }
}

// --- Binary protocol encoding helpers ---

fn encode_get(key: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(24 + key.len());
    encode_get_into(&mut buf, key);
    buf
}

fn encode_get_into(buf: &mut Vec<u8>, key: &str) {
    buf.reserve(24 + key.len());
    let key_bytes = key.as_bytes();
    let total_body_len = key_bytes.len() as u32;
    buf.push(REQUEST_MAGIC); // magic
    buf.push(0x00); // opcode: GET
    buf.extend_from_slice(&(key_bytes.len() as u16).to_be_bytes()); // key length
    buf.push(0); // extras length
    buf.push(0); // data type
    buf.extend_from_slice(&[0, 0]); // vbucket id
    buf.extend_from_slice(&total_body_len.to_be_bytes()); // total body length
    buf.extend_from_slice(&[0, 0, 0, 0]); // opaque
    buf.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0]); // CAS
    buf.extend_from_slice(key_bytes);
}

fn encode_set_into(buf: &mut Vec<u8>, key: &str, value: &[u8], flags: u32, exptime: u32) {
    buf.reserve(24 + key.len() + value.len() + 8);
    let key_bytes = key.as_bytes();
    let extras_len = 8;
    let total_body_len = (extras_len + key_bytes.len() + value.len()) as u32;
    buf.push(REQUEST_MAGIC);
    buf.push(0x01); // SET
    buf.extend_from_slice(&(key_bytes.len() as u16).to_be_bytes());
    buf.push(extras_len as u8);
    buf.push(0);
    buf.extend_from_slice(&[0, 0]);
    buf.extend_from_slice(&total_body_len.to_be_bytes());
    buf.extend_from_slice(&[0, 0, 0, 0]);
    buf.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0]);
    buf.extend_from_slice(&flags.to_be_bytes());
    buf.extend_from_slice(&exptime.to_be_bytes());
    buf.extend_from_slice(key_bytes);
    buf.extend_from_slice(value);
}

fn encode_set_quietly_into(buf: &mut Vec<u8>, key: &str, value: &[u8]) {
    buf.reserve(24 + key.len() + value.len() + 8);
    let key_bytes = key.as_bytes();
    let flags: u32 = 0;
    let exptime: u32 = 0;
    let extras_len = 8;
    let total_body_len = (extras_len + key_bytes.len() + value.len()) as u32;
    buf.push(REQUEST_MAGIC);
    buf.push(0x11); // SETQ
    buf.extend_from_slice(&(key_bytes.len() as u16).to_be_bytes());
    buf.push(extras_len as u8);
    buf.push(0);
    buf.extend_from_slice(&[0, 0]);
    buf.extend_from_slice(&total_body_len.to_be_bytes());
    buf.extend_from_slice(&[0, 0, 0, 0]);
    buf.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0]);
    buf.extend_from_slice(&flags.to_be_bytes());
    buf.extend_from_slice(&exptime.to_be_bytes());
    buf.extend_from_slice(key_bytes);
    buf.extend_from_slice(value);
}

fn encode_delete_into(buf: &mut Vec<u8>, key: &str) {
    buf.reserve(24 + key.len());
    let key_bytes = key.as_bytes();
    let total_body_len = key_bytes.len() as u32;
    buf.push(REQUEST_MAGIC); // magic
    buf.push(0x04); // opcode: DELETE
    buf.extend_from_slice(&(key_bytes.len() as u16).to_be_bytes()); // key length
    buf.push(0); // extras length
    buf.push(0); // data type
    buf.extend_from_slice(&[0, 0]); // vbucket id
    buf.extend_from_slice(&total_body_len.to_be_bytes()); // total body length
    buf.extend_from_slice(&[0, 0, 0, 0]); // opaque
    buf.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0]); // CAS
    buf.extend_from_slice(key_bytes);
}

fn encode_delete_quiet_into(buf: &mut Vec<u8>, key: &str) {
    buf.reserve(24 + key.len());
    let key_bytes = key.as_bytes();
    let total_body_len = key_bytes.len() as u32;
    buf.push(REQUEST_MAGIC); // magic
    buf.push(0x14); // opcode: DELETEQ
    buf.extend_from_slice(&(key_bytes.len() as u16).to_be_bytes()); // key length
    buf.push(0); // extras length
    buf.push(0); // data type
    buf.extend_from_slice(&[0, 0]); // vbucket id
    buf.extend_from_slice(&total_body_len.to_be_bytes()); // total body length
    buf.extend_from_slice(&[0, 0, 0, 0]); // opaque
    buf.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0]); // CAS
    buf.extend_from_slice(key_bytes);
}

fn encode_incr_decr_into(
    buf: &mut Vec<u8>,
    key: &str,
    delta: u64,
    initial: u64,
    exptime: u32,
    incr: bool,
) {
    buf.reserve(24 + key.len() + 20);
    let key_bytes = key.as_bytes();
    let extras_len = 20;
    let total_body_len = (extras_len + key_bytes.len()) as u32;
    buf.push(REQUEST_MAGIC);
    buf.push(if incr { 0x05 } else { 0x06 }); // INCR or DECR
    buf.extend_from_slice(&(key_bytes.len() as u16).to_be_bytes());
    buf.push(extras_len as u8);
    buf.push(0);
    buf.extend_from_slice(&[0, 0]);
    buf.extend_from_slice(&total_body_len.to_be_bytes());
    buf.extend_from_slice(&[0, 0, 0, 0]);
    buf.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0]);
    buf.extend_from_slice(&delta.to_be_bytes());
    buf.extend_from_slice(&initial.to_be_bytes());
    buf.extend_from_slice(&exptime.to_be_bytes());
    buf.extend_from_slice(key_bytes);
}

fn encode_noop_into(buf: &mut Vec<u8>) {
    buf.reserve(24);
    buf.push(REQUEST_MAGIC);
    buf.push(0x0a); // NOOP
    buf.extend_from_slice(&[0, 0]); // key length
    buf.push(0); // extras length
    buf.push(0); // data type
    buf.extend_from_slice(&[0, 0]); // vbucket id
    buf.extend_from_slice(&0u32.to_be_bytes()); // total body length
    buf.extend_from_slice(&[0, 0, 0, 0]); // opaque
    buf.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0]); // CAS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_set_and_get() {
        let client = MemcacheClient::connect("127.0.0.1:11211").await;
        client.set("foo", b"bar", 0, 0).await.expect("SET failed");
        let val = client.get("foo").await.expect("GET failed");
        assert_eq!(val, Some(b"bar".to_vec()));
    }

    #[tokio::test]
    async fn test_get_missing() {
        let client = MemcacheClient::connect("127.0.0.1:11211").await;
        let val = client.get("no_such_key").await.expect("GET failed");
        assert_eq!(val, None);
    }

    #[tokio::test]
    async fn test_set_multi_and_get_single_multiple_times() {
        let client = MemcacheClient::connect("127.0.0.1:11211").await;
        client
            .set_multi(
                vec![("foo", b"bar" as &[u8]), ("baz", b"qux"), ("num", b"42")],
                0,
                0,
            )
            .await
            .expect("set_multi failed");

        assert_eq!(client.get("foo").await.unwrap(), Some(b"bar".to_vec()));
        assert_eq!(client.get("baz").await.unwrap(), Some(b"qux".to_vec()));
        assert_eq!(client.get("num").await.unwrap(), Some(b"42".to_vec()));
    }

    #[tokio::test]
    async fn test_get_multi() {
        let client = MemcacheClient::connect("127.0.0.1:11211").await;
        client.set("foo", b"bar", 0, 0).await.unwrap();
        client.set("baz", b"qux", 0, 0).await.unwrap();

        let results = client
            .get_multi(vec!["foo", "baz", "missing"])
            .await
            .expect("get_multi failed");

        assert_eq!(results[0], ("foo".to_string(), Some(b"bar".to_vec())));
        assert_eq!(results[1], ("baz".to_string(), Some(b"qux".to_vec())));
        assert_eq!(results[2], ("missing".to_string(), None));
    }

    #[tokio::test]
    async fn test_set_multi_get_multi() {
        let client = MemcacheClient::connect("127.0.0.1:11211").await;
        client
            .set_multi(
                vec![("foo", b"bar" as &[u8]), ("baz", b"qux"), ("num", b"42")],
                0,
                0,
            )
            .await
            .expect("set_multi failed");

        let results = client
            .get_multi(vec!["foo", "baz", "num"])
            .await
            .expect("get_multi failed");

        assert_eq!(results[0], ("foo".to_string(), Some(b"bar".to_vec())));
        assert_eq!(results[1], ("baz".to_string(), Some(b"qux".to_vec())));
        assert_eq!(results[2], ("num".to_string(), Some(b"42".to_vec())));
    }

    #[tokio::test]
    async fn bench_set_1000() {
        let client = MemcacheClient::connect("127.0.0.1:11211").await;
        let start = std::time::Instant::now();
        for i in 0..1000 {
            let key = format!("bench_set_1000_{i}");
            client.set(&key, b"val", 0, 0).await.unwrap();
        }
        let elapsed = start.elapsed();
        println!("bench_set_1000: {:?}", elapsed);
        println!("per unit: {:?}", elapsed / 1000);
    }

    #[tokio::test]
    async fn bench_get_1000() {
        let client = MemcacheClient::connect("127.0.0.1:11211").await;
        // Pre-populate
        for i in 0..1000 {
            let key = format!("bench_get_1000_{i}");
            client.set(&key, b"val", 0, 0).await.unwrap();
        }
        let start = std::time::Instant::now();
        for i in 0..1000 {
            let key = format!("bench_get_1000_{i}");
            let val = client.get(&key).await.unwrap();
            assert_eq!(val, Some(b"val".to_vec()));
        }
        let elapsed = start.elapsed();
        println!("bench_get_1000: {:?}", elapsed);
        println!("per unit: {:?}", elapsed / 1000);
    }

    #[tokio::test]
    async fn bench_set_multi_1000() {
        let client = MemcacheClient::connect("127.0.0.1:11211").await;
        let items: Vec<_> = (0..100)
            .map(|i| (format!("bench_set_multi_1000_{i}"), b"val" as &[u8]))
            .collect();
        let items_ref: Vec<(&str, &[u8])> = items.iter().map(|(k, v)| (k.as_str(), *v)).collect();
        let start = std::time::Instant::now();
        client.set_multi(items_ref, 0, 0).await.unwrap();
        let elapsed = start.elapsed();
        println!("bench_set_multi_1000: {:?}", elapsed);
        println!("per unit: {:?}", elapsed / 1000);
    }

    #[tokio::test]
    async fn bench_get_multi_1000() {
        let client = MemcacheClient::connect("127.0.0.1:11211").await;
        // Pre-populate
        let items: Vec<_> = (0..1000)
            .map(|i| (format!("bench_get_multi_1000_{i}"), b"val" as &[u8]))
            .collect();
        let items_ref: Vec<(&str, &[u8])> = items.iter().map(|(k, v)| (k.as_str(), *v)).collect();
        client.set_multi(items_ref, 0, 0).await.unwrap();

        let keys: Vec<_> = (0..100)
            .map(|i| format!("bench_get_multi_1000_{i}"))
            .collect();
        let keys_ref: Vec<&str> = keys.iter().map(|k| k.as_str()).collect();

        let start = std::time::Instant::now();
        let results = client.get_multi(keys_ref).await.unwrap();
        for (_k, v) in results {
            assert_eq!(v, Some(b"val".to_vec()));
        }
        let elapsed = start.elapsed();
        println!("bench_get_multi_1000: {:?}", elapsed);
        println!("per unit: {:?}", elapsed / 1000);
    }

    #[tokio::test]
    async fn test_set_quietly_and_get() {
        let client = MemcacheClient::connect("127.0.0.1:11211").await;
        client.set_quietly("quiet_key", b"quiet_val").await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let val = client.get("quiet_key").await.unwrap();
        assert_eq!(val, Some(b"quiet_val".to_vec()));
    }

    #[tokio::test]
    async fn bench_set_quietly_1000() {
        let client = MemcacheClient::connect("127.0.0.1:11211").await;
        let start = std::time::Instant::now();
        for i in 0..1000 {
            let key = format!("bench_set_quietly_1000_{i}");
            client.set_quietly(&key, b"val").await.unwrap();
        }
        let elapsed = start.elapsed();
        println!("bench_set_quietly_1000: {:?}", elapsed);
        println!("per unit: {:?}", elapsed / 1000);
    }

    #[tokio::test]
    async fn test_set_multi_quiet_and_get() {
        let client = MemcacheClient::connect("127.0.0.1:11211").await;
        client
            .set_multi_quietly(vec![
                ("quiet_batch_foo", b"bar" as &[u8]),
                ("quiet_batch_baz", b"qux"),
                ("quiet_batch_num", b"42"),
            ])
            .await
            .expect("set_multi_quiet failed");
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert_eq!(
            client.get("quiet_batch_foo").await.unwrap(),
            Some(b"bar".to_vec())
        );
        assert_eq!(
            client.get("quiet_batch_baz").await.unwrap(),
            Some(b"qux".to_vec())
        );
        assert_eq!(
            client.get("quiet_batch_num").await.unwrap(),
            Some(b"42".to_vec())
        );
    }

    #[tokio::test]
    async fn test_set_multi_quiet_empty() {
        let client = MemcacheClient::connect("127.0.0.1:11211").await;
        client
            .set_multi_quietly(Vec::<(&str, &[u8])>::new())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_set_multi_quiet_single() {
        let client = MemcacheClient::connect("127.0.0.1:11211").await;
        client
            .set_multi_quietly(vec![("quiet_batch_single", b"only" as &[u8])])
            .await
            .expect("set_multi_quiet failed");
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert_eq!(
            client.get("quiet_batch_single").await.unwrap(),
            Some(b"only".to_vec())
        );
    }

    #[tokio::test]
    async fn bench_set_multi_quiet_1000() {
        let client = MemcacheClient::connect("127.0.0.1:11211").await;
        let items: Vec<_> = (0..1000)
            .map(|i| (format!("bench_set_multi_quiet_1000_{i}"), b"val" as &[u8]))
            .collect();
        let items_ref: Vec<(&str, &[u8])> = items.iter().map(|(k, v)| (k.as_str(), *v)).collect();
        let start = std::time::Instant::now();
        client.set_multi_quietly(items_ref).await.unwrap();
        let elapsed = start.elapsed();
        println!("bench_set_multi_quiet_1000: {:?}", elapsed);
        println!("per unit: {:?}", elapsed / 1000);
    }

    #[tokio::test]
    async fn bench_set_multi_quiet_three_pods_1000_times() {
        let client = MemcacheClient::connect("127.0.0.1:11211").await;

        // Prepare 3 pods with fixed trade_side and entry_price values
        let pods = [
            (0u8, 12345678f64, "bench_ts_key_0", "bench_ep_key_0"),
            (1u8, 22345678f64, "bench_ts_key_1", "bench_ep_key_1"),
            (2u8, 32345678f64, "bench_ts_key_2", "bench_ep_key_2"),
        ];

        let start = std::time::Instant::now();

        for _i in 0..1000 {
            let mut vec_to_persist: Vec<(&str, Vec<u8>)> = Vec::with_capacity(6);
            for (trade_side, entry_price, trade_side_key, entry_price_key) in &pods {
                vec_to_persist.push((*trade_side_key, trade_side.to_le_bytes().to_vec()));
                vec_to_persist.push((*entry_price_key, entry_price.to_le_bytes().to_vec()));
            }
            let vec_to_persist_refs: Vec<(&str, &[u8])> = vec_to_persist
                .iter()
                .map(|(k, v)| (*k, v.as_slice()))
                .collect();

            client.set_multi_quietly(vec_to_persist_refs).await.unwrap();
        }

        let elapsed = start.elapsed();
        println!(
            "bench_set_multi_quiet_three_pods_1000_times: {:?} ({} sets of 3 pods, {} keys total)",
            elapsed,
            1000,
            1000 * 6
        );
        println!("per unit: {:?}", elapsed / 1000);
    }

    #[tokio::test]
    async fn test_delete_and_get() {
        let client = MemcacheClient::connect("127.0.0.1:11211").await;
        client.set("delkey", b"delval", 0, 0).await.unwrap();
        assert_eq!(
            client.get("delkey").await.unwrap(),
            Some(b"delval".to_vec())
        );
        client.delete("delkey").await.unwrap();
        assert_eq!(client.get("delkey").await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_delete_quietly_and_get() {
        let client = MemcacheClient::connect("127.0.0.1:11211").await;
        client.set("delqkey", b"delqval", 0, 0).await.unwrap();
        assert_eq!(
            client.get("delqkey").await.unwrap(),
            Some(b"delqval".to_vec())
        );
        client.delete_quietly("delqkey").await.unwrap();
        // Give memcached a moment to process
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert_eq!(client.get("delqkey").await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_delete_multi_and_get() {
        let client = MemcacheClient::connect("127.0.0.1:11211").await;
        let keys = vec!["delmkey1", "delmkey2", "delmkey3"];
        for k in &keys {
            client.set(k, b"val", 0, 0).await.unwrap();
        }
        client.delete_multi(keys.clone()).await.unwrap();
        for k in &keys {
            assert_eq!(client.get(k).await.unwrap(), None);
        }
    }

    #[tokio::test]
    async fn test_delete_multi_quiet_and_get() {
        let client = MemcacheClient::connect("127.0.0.1:11211").await;
        let keys = vec!["delmqkey1", "delmqkey2", "delmqkey3"];
        for k in &keys {
            client.set(k, b"val", 0, 0).await.unwrap();
        }
        client.delete_multi_quietly(keys.clone()).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        for k in &keys {
            assert_eq!(client.get(k).await.unwrap(), None);
        }
    }

    #[tokio::test]
    async fn bench_delete_1000() {
        let client = MemcacheClient::connect("127.0.0.1:11211").await;
        // Pre-populate
        for i in 0..1000 {
            let key = format!("bench_delete_1000_{i}");
            client.set(&key, b"val", 0, 0).await.unwrap();
        }
        let start = std::time::Instant::now();
        for i in 0..1000 {
            let key = format!("bench_delete_1000_{i}");
            client.delete(&key).await.unwrap();
        }
        let elapsed = start.elapsed();
        println!("bench_delete_1000: {:?}", elapsed);
        println!("per unit: {:?}", elapsed / 1000);
    }

    #[tokio::test]
    async fn bench_delete_quietly_1000() {
        let client = MemcacheClient::connect("127.0.0.1:11211").await;
        // Pre-populate
        for i in 0..1000 {
            let key = format!("bench_delete_quietly_1000_{i}");
            client.set(&key, b"val", 0, 0).await.unwrap();
        }
        let start = std::time::Instant::now();
        for i in 0..1000 {
            let key = format!("bench_delete_quietly_1000_{i}");
            client.delete_quietly(&key).await.unwrap();
        }
        let elapsed = start.elapsed();
        println!("bench_delete_quietly_1000: {:?}", elapsed);
        println!("per unit: {:?}", elapsed / 1000);
    }

    #[tokio::test]
    async fn bench_delete_multi_1000() {
        let client = MemcacheClient::connect("127.0.0.1:11211").await;
        // Pre-populate
        let keys: Vec<_> = (0..1000)
            .map(|i| format!("bench_delete_multi_1000_{i}"))
            .collect();
        for k in &keys {
            client.set(k, b"val", 0, 0).await.unwrap();
        }
        let keys_ref: Vec<&str> = keys.iter().map(|k| k.as_str()).collect();
        let start = std::time::Instant::now();
        client.delete_multi(keys_ref).await.unwrap();
        let elapsed = start.elapsed();
        println!("bench_delete_multi_1000: {:?}", elapsed);
        println!("per unit: {:?}", elapsed / 1000);
    }

    #[tokio::test]
    async fn bench_delete_multi_quiet_1000() {
        let client = MemcacheClient::connect("127.0.0.1:11211").await;
        // Pre-populate
        let keys: Vec<_> = (0..1000)
            .map(|i| format!("bench_delete_multi_quiet_1000_{i}"))
            .collect();
        for k in &keys {
            client.set(k, b"val", 0, 0).await.unwrap();
        }
        let keys_ref: Vec<&str> = keys.iter().map(|k| k.as_str()).collect();
        let start = std::time::Instant::now();
        client.delete_multi_quietly(keys_ref).await.unwrap();
        let elapsed = start.elapsed();
        println!("bench_delete_multi_quiet_1000: {:?}", elapsed);
        println!("per unit: {:?}", elapsed / 1000);
    }

    #[tokio::test]
    async fn test_incr_decr() {
        let client = MemcacheClient::connect("127.0.0.1:11211").await;

        client.set("incrkey", b"42", 0, 0).await.unwrap();
        let val = client.incr("incrkey", 8, 0, 0).await.unwrap();
        assert_eq!(val, Some(50));
        let val = client.decr("incrkey", 10, 0, 0).await.unwrap();
        assert_eq!(val, Some(40));
        // Not found
        let val = client.incr("missingkey", 1, 123, 0).await.unwrap();
        assert_eq!(val, Some(123));
        client.delete("incrkey").await.unwrap();
        client.delete("missingkey").await.unwrap();
    }

    #[tokio::test]
    async fn test_noop() {
        let client = MemcacheClient::connect("127.0.0.1:11211").await;
        client.noop().await.unwrap();
    }

    #[tokio::test]
    async fn bench_incr_1000() {
        let client = MemcacheClient::connect("127.0.0.1:11211").await;
        client.set("bench_incr", b"0", 0, 0).await.unwrap();
        let start = std::time::Instant::now();
        let mut last = 0;
        for _ in 0..1000 {
            last = client.incr("bench_incr", 1, 0, 0).await.unwrap().unwrap();
        }
        let elapsed = start.elapsed();
        println!("bench_incr_1000: {:?}, last value: {}", elapsed, last);
    }

    #[tokio::test]
    async fn bench_decr_1000() {
        let client = MemcacheClient::connect("127.0.0.1:11211").await;
        client.set("bench_decr", b"1000", 0, 0).await.unwrap();
        let start = std::time::Instant::now();
        let mut last = 0;
        for _ in 0..1000 {
            last = client.decr("bench_decr", 1, 0, 0).await.unwrap().unwrap();
        }
        let elapsed = start.elapsed();
        println!("bench_decr_1000: {:?}, last value: {}", elapsed, last);
    }

    #[tokio::test]
    async fn bench_noop_1000() {
        let client = MemcacheClient::connect("127.0.0.1:11211").await;
        let start = std::time::Instant::now();
        for _ in 0..1000 {
            client.noop().await.unwrap();
        }
        let elapsed = start.elapsed();
        println!("bench_noop_1000: {:?}", elapsed);
    }
}
