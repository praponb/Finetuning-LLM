WITH unpacked AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        items,
        ship_to,
        ARRAY_MAX(TRANSFORM(items, i -> i.price)) AS max_item_price,
        COALESCE(attributes['gift'], 'none') AS gift_or_default,
        EXISTS(coupon_codes, c -> c LIKE 'W%') AS has_welcome_code,
        attributes['device'] AS device
    FROM carts
)
SELECT
    id,
    coupon_codes,
    ROUND(max_item_price / SUM(max_item_price) OVER (PARTITION BY DATE_TRUNC('YEAR', created_at), customer_id), 4) AS max_item_price_share,
    LEAD(max_item_price, 2) OVER (ORDER BY max_item_price, id) AS next_max_item_price,
    ROUND(max_item_price / SUM(max_item_price) OVER (PARTITION BY DATE_TRUNC('YEAR', created_at), customer_id), 4) AS max_item_price_share_2,
    NTILE(4) OVER (PARTITION BY MONTH(created_at), customer_id ORDER BY max_item_price DESC, id) AS max_item_price_bucket,
    SUM(max_item_price) OVER (PARTITION BY MONTH(created_at), customer_id ORDER BY created_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_item_price
FROM unpacked;
