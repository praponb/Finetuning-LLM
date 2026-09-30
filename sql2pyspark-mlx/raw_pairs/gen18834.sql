WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        key,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) t AS key, attr_value
)
-- Multi-level aggregation
SELECT
    customer_id,
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS duration_min,
    SIZE(COLLECT_SET(key)) AS num_keys,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS duration_min_2,
    COUNT_IF(key = 'utm_source') AS utm_source_events,
    COUNT(CASE WHEN key = 'weight_kg' THEN 1 END) AS weight_kg_count,
    COUNT(*) FILTER (WHERE key = 'gift') AS gift_rows
FROM exploded
GROUP BY GROUPING SETS ((customer_id, DATE_TRUNC('HOUR', created_at)), (customer_id), ())
HAVING duration_min > 0
ORDER BY weight_kg_count NULLS LAST;
