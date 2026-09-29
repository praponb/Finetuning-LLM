SELECT
    id,
    host,
    latency_ms,
    status_code,
    k,
    attr_value
FROM app_logs
LATERAL VIEW EXPLODE(context) e AS k, attr_value
