SELECT
    id,
    customer_id,
    created_at,
    coupon_codes,
    items,
    ship_to,
    attributes,
    COALESCE(ship_to.zip, 'UNKNOWN') AS zip_or_unknown,
    attributes['gift'] AS gift,
    COALESCE(attributes['gift'], 'unknown') AS gift_or_default,
    ship_to.zip AS ship_zip,
    TRANSFORM(
        FILTER(items, x -> x.promo IS NOT NULL),
        x -> STRUCT(x.product_id AS pid, ROUND(x.price * 0.9, 2) AS discounted_price)
    ) AS promo_lines
FROM carts
WHERE ARRAY_CONTAINS(coupon_codes, 'VIP');
