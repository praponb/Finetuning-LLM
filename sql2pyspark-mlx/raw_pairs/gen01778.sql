select id, log_ts, attr_key, attr_value from app_logs lateral view explode(context) li as attr_key, attr_value
