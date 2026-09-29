WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS key, value
)
SELECT
    id,
    host,
    log_ts,
    level,
    key,
    value,
    CASE WHEN host RLIKE '^(h1|h2)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS host_zone
FROM lines
