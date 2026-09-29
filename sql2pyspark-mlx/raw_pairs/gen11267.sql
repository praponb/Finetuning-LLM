SELECT
    id,
    level,
    latency_ms,
    key,
    v
FROM app_logs
LATERAL VIEW OUTER EXPLODE(context) e AS key, v
