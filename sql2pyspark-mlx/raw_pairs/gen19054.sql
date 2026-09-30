SELECT id, service, host, log_ts, level, latency_ms, status_code, key, value FROM app_logs LATERAL VIEW OUTER EXPLODE(context) e AS key, value;
