WITH flattened AS (
    SELECT
        id,
        host,
        log_ts,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS key, value
)
SELECT
    id,
    log_ts,
    status_code,
    key,
    value
FROM flattened
