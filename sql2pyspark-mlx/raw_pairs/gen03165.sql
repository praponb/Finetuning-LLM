SELECT
    id,
    service,
    host,
    log_ts,
    level,
    latency_ms,
    k,
    v
FROM app_logs
LATERAL VIEW EXPLODE(context) t AS k, v
