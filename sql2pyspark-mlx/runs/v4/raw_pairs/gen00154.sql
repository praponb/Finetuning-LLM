WITH parsed AS (
    -- Semi-structured parsing
    SELECT
        id,
        created_at,
        coupon_codes,
        items,
        attributes,
        EXISTS(items, li -> li.price > 100) AS has_premium_item,
        SIZE(attributes) AS num_attributes
    FROM carts
)
-- Grouped metrics
SELECT
    CASE WHEN num_attributes >= 90 THEN 'LARGE' ELSE 'SMALL' END AS num_attributes_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS duration_min,
    MAX(created_at) AS last_seen
FROM parsed
GROUP BY num_attributes_tier
ORDER BY distinct_ids DESC NULLS LAST
