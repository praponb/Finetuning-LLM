SELECT id, service, log_ts, level, latency_ms, attr_key, v FROM app_logs LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, v;
