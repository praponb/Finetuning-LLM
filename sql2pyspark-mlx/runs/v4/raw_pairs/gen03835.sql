SELECT id, service, host, log_ts, status_code, key, value FROM app_logs LATERAL VIEW OUTER EXPLODE(context) e AS key, value
