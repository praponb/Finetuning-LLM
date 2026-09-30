SELECT id, service, host, log_ts, latency_ms, status_code, attr_key, v FROM app_logs LATERAL VIEW EXPLODE(context) t AS attr_key, v;
