SELECT
    id,
    customer_id,
    created_at,
    ship_to,
    attributes,
    TRANSFORM(items, x -> x.product_id) AS product_ids,
    AGGREGATE(items, CAST(0 AS DOUBLE), (acc, x) -> acc + x.qty * x.price, acc -> ROUND(acc, 2)) AS items_total,
    TRANSFORM(
        FILTER(items, x -> x.promo IS NOT NULL),
        x -> STRUCT(x.product_id AS pid, ROUND(x.price * 0.9, 2) AS discounted_price)
    ) AS promo_lines,
    SIZE(attributes) AS num_attributes,
    COALESCE(ship_to.city, 'UNKNOWN') AS city_or_unknown
FROM carts;
