WITH unpacked AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        ship_to,
        COALESCE(attributes['gift'], 'direct') AS gift_or_default,
        EXISTS(items, x -> x.price > 100) AS has_premium_item,
        FORALL(items, x -> x.qty <= 5) AS all_small_qty
    FROM carts
)
-- Grouped metrics
SELECT
    DATE_TRUNC('DAY', created_at) AS activity_day,
    customer_id,
    MAX(created_at) AS last_seen,
    COUNT(*) AS num_rows
FROM unpacked
GROUP BY GROUPING SETS ((DATE_TRUNC('DAY', created_at), customer_id), (DATE_TRUNC('DAY', created_at)), ())
ORDER BY num_rows DESC NULLS LAST
