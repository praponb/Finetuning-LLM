SELECT id, service, host, log_ts, latency_ms, status_code, attr_key, attr_value FROM app_logs LATERAL VIEW EXPLODE(context) e AS attr_key, attr_value;
