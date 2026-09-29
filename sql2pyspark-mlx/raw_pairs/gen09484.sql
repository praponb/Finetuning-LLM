WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        EXPLODE_OUTER(coupon_codes) AS tag
    FROM carts
)
-- Grouped metrics
SELECT
    DATE_TRUNC('DAY', created_at) AS activity_day,
    tag,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) FILTER (WHERE tag = 'BUNDLE') AS bundle_rows,
    COUNT(*) FILTER (WHERE tag = 'SPRING10') AS spring10_rows
FROM lines
GROUP BY GROUPING SETS ((DATE_TRUNC('DAY', created_at), tag), (DATE_TRUNC('DAY', created_at)))
ORDER BY bundle_rows DESC;
