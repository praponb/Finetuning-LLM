WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        level,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS k, v
)
SELECT id, k
FROM lines
ORDER BY id
