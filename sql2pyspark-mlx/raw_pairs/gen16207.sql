SELECT
    id,
    created_at,
    coupon_codes,
    items,
    ship_to,
    attributes,
    attributes['utm_source'] AS utm_source,
    FORALL(items, i -> i.qty <= 5) AS all_small_qty,
    TRANSFORM(
        FILTER(items, i -> i.promo IS NOT NULL),
        i -> STRUCT(i.product_id AS pid, ROUND(i.price * 0.9, 2) AS discounted_price)
    ) AS promo_lines,
    COALESCE(attributes['gift'], 'none') AS gift_or_default,
    EXISTS(items, i -> i.price > 100) AS has_premium_item
FROM carts
WHERE ARRAY_CONTAINS(coupon_codes, 'VIP');
