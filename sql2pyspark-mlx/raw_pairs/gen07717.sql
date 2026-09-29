SELECT
    id,
    customer_id,
    created_at,
    coupon_codes,
    k,
    v
FROM carts
LATERAL VIEW OUTER EXPLODE(attributes) e AS k, v;
