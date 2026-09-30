WITH unpacked AS (
    -- Higher-order array functions
    SELECT
        id,
        coupon_codes,
        ship_to,
        COALESCE(attributes['device'], 'direct') AS device_or_default,
        ARRAY_CONTAINS(coupon_codes, 'VIP') AS used_vip,
        SIZE(items) AS num_items
    FROM carts
)
-- Grouped metrics
SELECT
    CASE WHEN num_items >= 3 THEN 'HIGH' ELSE 'LOW' END AS num_items_tier,
    SUM(num_items) AS total_num_items,
    APPROX_PERCENTILE(num_items, 0.5) AS num_items_p50,
    MAX(num_items) AS max_num_items
FROM unpacked
GROUP BY CASE WHEN num_items >= 3 THEN 'HIGH' ELSE 'LOW' END
