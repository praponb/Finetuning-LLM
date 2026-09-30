WITH lines AS (SELECT id, service, host, log_ts, level, latency_ms, k, v FROM app_logs LATERAL VIEW EXPLODE(context) t AS k, v) SELECT id, service, host, level, latency_ms, k, v FROM lines ORDER BY k
