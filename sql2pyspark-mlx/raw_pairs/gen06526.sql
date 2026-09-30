WITH lines AS (
    SELECT
        id,
        host,
        log_ts,
        level,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, value
)
SELECT
    id,
    host,
    log_ts,
    level,
    status_code,
    k,
    value
FROM lines
