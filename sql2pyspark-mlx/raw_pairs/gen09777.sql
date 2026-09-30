SELECT id, service, log_ts, status_code, key, v FROM app_logs LATERAL VIEW EXPLODE(context) e AS key, v
