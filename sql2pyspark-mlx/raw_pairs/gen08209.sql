WITH lines AS (SELECT id, service, log_ts, level, latency_ms, key, v FROM app_logs LATERAL VIEW OUTER EXPLODE(context) t AS key, v) SELECT log_ts, latency_ms, v FROM lines
