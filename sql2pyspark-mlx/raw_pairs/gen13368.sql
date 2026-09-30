WITH unpacked AS (
    -- Semi-structured parsing
    SELECT
        id,
        customer_id,
        created_at,
        attributes,
        EXISTS(items, x -> x.price > 100) AS has_premium_item,
        ARRAY_MAX(TRANSFORM(items, x -> x.price)) AS max_item_price,
        attributes['gift'] AS gift,
        EXISTS(coupon_codes, c -> c LIKE 'W%') AS has_welcome_code
    FROM carts
)
-- Running and moving aggregates
SELECT
    *,
    LEAD(max_item_price, 1, 0) OVER (PARTITION BY customer_id ORDER BY created_at, id) AS next_max_item_price,
    RANK() OVER (PARTITION BY customer_id ORDER BY max_item_price NULLS FIRST) AS max_item_price_rank
FROM unpacked;
