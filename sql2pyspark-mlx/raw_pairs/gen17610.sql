select id, log_ts, latency_ms, status_code, attr_key, value from app_logs lateral view outer explode(context) e as attr_key, value
