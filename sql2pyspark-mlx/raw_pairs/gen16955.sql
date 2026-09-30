SELECT id, service, host, log_ts, latency_ms, status_code, key, attr_value FROM app_logs LATERAL VIEW OUTER EXPLODE(context) t AS key, attr_value
