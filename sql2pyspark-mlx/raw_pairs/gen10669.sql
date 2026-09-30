SELECT
    id,
    coupon_codes,
    k,
    v
FROM carts
LATERAL VIEW OUTER EXPLODE(attributes) x AS k, v;
