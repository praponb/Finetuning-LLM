WITH flattened AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS attr_key, v
)
SELECT
    id,
    service,
    log_ts,
    level,
    attr_key,
    v
FROM flattened
ORDER BY level, id;
