SELECT id, service, host, log_ts, level, status_code, k, attr_value FROM app_logs LATERAL VIEW EXPLODE(context) x AS k, attr_value;
