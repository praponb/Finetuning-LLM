SELECT id, service, host, log_ts, latency_ms, status_code, k, value FROM app_logs LATERAL VIEW EXPLODE(context) e AS k, value;
