SELECT
    id,
    customer_id,
    coupon_codes,
    ship_to,
    ship_to.country AS ship_country,
    COALESCE(attributes['gift'], 'unknown') AS gift_or_default,
    FORALL(items, item -> item.qty <= 5) AS all_small_qty,
    COALESCE(attributes['device'], 'none') AS device_or_default,
    SIZE(FILTER(items, item -> item.qty >= 3)) AS bulk_lines
FROM carts
WHERE ARRAY_CONTAINS(coupon_codes, 'VIP');
