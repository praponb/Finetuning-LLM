select id, service, log_ts, level, latency_ms, k, attr_value from app_logs lateral view outer explode(context) li as k, attr_value
