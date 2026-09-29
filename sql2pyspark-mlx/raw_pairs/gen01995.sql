WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        latency_ms,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS k, value
)
SELECT
    id,
    service,
    host,
    latency_ms,
    value,
    CASE WHEN host RLIKE '^(h2|h1)$' THEN 'EXTENDED' ELSE 'PRIORITY' END AS host_zone
FROM exploded
ORDER BY host DESC
