SELECT
    id,
    level,
    latency_ms,
    attr_key,
    attr_value
FROM app_logs
LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, attr_value
