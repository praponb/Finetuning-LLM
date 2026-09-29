SELECT
    id,
    customer_id,
    coupon_codes,
    EXISTS(coupon_codes, c -> c LIKE 'W%') AS has_welcome_code,
    ARRAY_CONTAINS(coupon_codes, 'SPRING10') AS used_spring10,
    TRY_CAST(attributes['weight_kg'] AS DOUBLE) AS weight_kg
FROM carts
WHERE customer_id IN (8, 13);
