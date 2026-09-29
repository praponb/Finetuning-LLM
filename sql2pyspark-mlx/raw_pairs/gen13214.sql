WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        level,
        latency_ms,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, attr_value
)
SELECT
    id,
    level,
    latency_ms,
    k,
    attr_value
FROM flattened
ORDER BY k DESC
