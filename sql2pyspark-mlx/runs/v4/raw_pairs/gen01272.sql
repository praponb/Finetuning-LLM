select id, host, level, status_code, k, value from app_logs lateral view outer explode(context) li as k, value
