WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS key, v
)
SELECT
    id,
    host,
    status_code,
    key,
    v
FROM lines
ORDER BY status_code DESC NULLS LAST
