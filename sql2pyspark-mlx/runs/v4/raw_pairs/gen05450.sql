SELECT id, log_ts, level, status_code, key, attr_value FROM app_logs LATERAL VIEW EXPLODE(context) li AS key, attr_value
