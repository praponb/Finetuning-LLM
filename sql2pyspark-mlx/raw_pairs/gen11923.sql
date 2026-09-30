SELECT id, service, log_ts, key, v FROM app_logs LATERAL VIEW EXPLODE(context) x AS key, v;
