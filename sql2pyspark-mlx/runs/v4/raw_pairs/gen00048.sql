SELECT
    id,
    customer_id,
    created_at,
    coupon_codes,
    attr_key,
    v
FROM carts
LATERAL VIEW EXPLODE(attributes) e AS attr_key, v;
