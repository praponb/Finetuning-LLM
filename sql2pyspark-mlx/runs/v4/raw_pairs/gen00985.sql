WITH exploded AS (
    SELECT
        id,
        created_at,
        key,
        attr_value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) x AS key, attr_value
)
SELECT
    key,
    COUNT(*) FILTER (WHERE key = 'utm_source') AS utm_source_rows,
    COUNT(DISTINCT key) AS distinct_keys,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS span_minutes,
    COUNT(*) AS cnt,
    MIN(created_at) AS first_created_at,
    COUNT_IF(key = 'utm_source') AS utm_source_events
FROM exploded
GROUP BY key
