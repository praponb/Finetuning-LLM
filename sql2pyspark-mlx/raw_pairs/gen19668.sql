SELECT id, log_ts, level, latency_ms, status_code, key, v FROM app_logs LATERAL VIEW OUTER EXPLODE(context) t AS key, v;
