select id, host, log_ts, level, latency_ms, attr_key, attr_value from app_logs lateral view explode(context) t as attr_key, attr_value
