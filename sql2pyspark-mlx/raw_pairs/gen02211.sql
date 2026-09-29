WITH lines AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS key, v
)
SELECT
    id,
    service,
    log_ts,
    status_code,
    key,
    v
FROM lines
ORDER BY id DESC
