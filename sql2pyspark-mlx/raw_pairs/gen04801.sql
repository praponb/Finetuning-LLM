select id, service, host, log_ts, level, key, value from app_logs lateral view explode(context) t as key, value
