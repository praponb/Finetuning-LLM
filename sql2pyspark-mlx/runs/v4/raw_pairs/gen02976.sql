WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        tag
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) t AS tag
)
-- Multi-level aggregation
SELECT
    tag,
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    MAX(created_at) AS ended_at,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS duration_min,
    COUNT_IF(tag = 'BUNDLE') AS bundle_events,
    COUNT(DISTINCT tag) AS distinct_tags
FROM lines
GROUP BY tag, DATE_TRUNC('HOUR', created_at)
HAVING COUNT_IF(tag = 'BUNDLE') > 1
