SELECT
    id,
    customer_id,
    created_at,
    items,
    ship_to,
    attributes,
    ARRAY_CONTAINS(coupon_codes, 'SPRING10') AS used_spring10,
    AGGREGATE(items, CAST(0 AS DOUBLE), (acc, i) -> acc + i.qty * i.price, acc -> ROUND(acc, 2)) AS cart_value,
    EXISTS(coupon_codes, c -> c LIKE 'W%') AS has_welcome_code,
    TRANSFORM(
        FILTER(items, i -> i.promo IS NOT NULL),
        i -> STRUCT(i.product_id AS pid, ROUND(i.price * 0.9, 2) AS discounted_price)
    ) AS promo_lines
FROM carts
