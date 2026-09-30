SELECT id, latency_ms, attr_key, value FROM app_logs LATERAL VIEW OUTER EXPLODE(context) x AS attr_key, value;
