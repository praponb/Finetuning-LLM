SELECT
    id,
    customer_id,
    created_at,
    coupon_codes,
    attributes['device'] AS device,
    ship_to.city AS ship_city,
    ARRAY_CONTAINS(coupon_codes, 'BUNDLE') AS used_bundle,
    EXISTS(items, i -> i.price > 100) AS has_premium_item
FROM carts
WHERE ARRAY_CONTAINS(coupon_codes, 'WELCOME5');
