SELECT
    id,
    customer_id,
    items,
    attributes,
    AGGREGATE(items, 0.0D, (acc, i) -> acc + i.qty * i.price, acc -> ROUND(acc, 2)) AS cart_value,
    EXISTS(items, i -> i.price > 100) AS has_premium_item,
    ARRAY_CONTAINS(coupon_codes, 'SPRING10') AS used_spring10,
    ship_to.city AS ship_city,
    FORALL(items, i -> i.qty <= 5) AS all_small_qty
FROM carts;
