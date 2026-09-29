WITH lines AS (
    -- Unnest the array into one row per element
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
    level,
    key,
    v
FROM lines
ORDER BY log_ts DESC NULLS FIRST;
