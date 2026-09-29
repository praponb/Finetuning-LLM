SELECT
    id,
    service,
    host,
    level,
    latency_ms,
    status_code,
    key,
    value
FROM app_logs
LATERAL VIEW OUTER EXPLODE(context) x AS key, value;
