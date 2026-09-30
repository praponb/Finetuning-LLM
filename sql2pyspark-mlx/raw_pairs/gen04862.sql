SELECT
    id,
    service,
    level,
    latency_ms,
    status_code,
    k,
    value
FROM app_logs
LATERAL VIEW EXPLODE(context) li AS k, value
