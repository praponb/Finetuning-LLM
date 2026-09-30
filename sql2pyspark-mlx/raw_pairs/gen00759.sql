SELECT id, host, level, latency_ms, key, v FROM app_logs LATERAL VIEW EXPLODE(context) li AS key, v
