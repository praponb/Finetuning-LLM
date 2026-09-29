SELECT id, host, level, latency_ms, k, attr_value FROM app_logs LATERAL VIEW OUTER EXPLODE(context) x AS k, attr_value;
