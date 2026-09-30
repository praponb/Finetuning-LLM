select id, log_ts, level, latency_ms, status_code, key, attr_value from app_logs lateral view explode(context) t as key, attr_value
