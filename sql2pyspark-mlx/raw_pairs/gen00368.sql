SELECT
    id,
    customer_id,
    created_at,
    coupon_codes,
    attributes,
    TRY_CAST(attributes['weight_kg'] AS DOUBLE) AS weight_kg,
    TRANSFORM(items, x -> x.product_id) AS product_ids,
    TRANSFORM(
        FILTER(items, x -> x.promo IS NOT NULL),
        x -> STRUCT(x.product_id AS pid, ROUND(x.price * 0.9, 2) AS discounted_price)
    ) AS promo_lines,
    ARRAY_CONTAINS(MAP_KEYS(attributes), 'gift') AS has_gift
FROM carts
