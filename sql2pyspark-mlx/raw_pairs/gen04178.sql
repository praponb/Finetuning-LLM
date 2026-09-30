SELECT
    id,
    latency_ms,
    status_code,
    attr_key,
    attr_value
FROM app_logs
LATERAL VIEW OUTER EXPLODE(context) x AS attr_key, attr_value
