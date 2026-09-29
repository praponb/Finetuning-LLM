WITH lines AS (
    SELECT
        id,
        log_ts,
        level,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS k, value
)
SELECT
    level,
    status_code,
    k,
    value
FROM lines
ORDER BY level DESC;
