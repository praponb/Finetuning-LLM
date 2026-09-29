SELECT
    id,
    created_at,
    ship_to,
    FORALL(items, item -> item.qty <= 5) AS all_small_qty,
    ARRAY_CONTAINS(coupon_codes, 'WELCOME5') AS used_welcome5,
    attributes['device'] AS device
FROM carts
WHERE HOUR(created_at) BETWEEN 10 AND 14
