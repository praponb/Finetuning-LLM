WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS key, value
)
SELECT
    id,
    host,
    log_ts,
    latency_ms,
    status_code,
    key,
    CASE WHEN key RLIKE '^(trace|region)$' THEN 'PRIORITY' WHEN key RLIKE '^(version)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS key_zone
FROM exploded
ORDER BY host
