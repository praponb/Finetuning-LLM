SELECT
    id,
    created_at,
    coupon_codes,
    items,
    attributes,
    ship_to.city AS ship_city,
    TRY_CAST(attributes['weight_kg'] AS DOUBLE) AS weight_kg
FROM carts
WHERE HOUR(created_at) BETWEEN 9 AND 16;
