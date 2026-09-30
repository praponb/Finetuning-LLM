WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        level,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS key, v
)
SELECT
    id,
    service,
    level,
    key,
    v
FROM flattened
ORDER BY level DESC, id
