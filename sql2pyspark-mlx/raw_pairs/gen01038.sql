SELECT
    id,
    host,
    level,
    latency_ms,
    key,
    value
FROM app_logs
LATERAL VIEW OUTER EXPLODE(context) t AS key, value;
