select id, service, host, level, latency_ms, status_code, key, value from app_logs lateral view outer explode(context) li as key, value;
