SELECT
    id,
    service,
    host,
    log_ts,
    level,
    latency_ms,
    status_code,
    key,
    v
FROM app_logs
LATERAL VIEW EXPLODE(context) li AS key, v
