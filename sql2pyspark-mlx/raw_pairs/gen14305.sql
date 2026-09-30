WITH flattened AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS k, attr_value
)
SELECT
    id,
    service,
    log_ts,
    level,
    latency_ms,
    k
FROM flattened
