SELECT
    id,
    customer_id,
    coupon_codes,
    attr_key,
    v
FROM carts
LATERAL VIEW OUTER EXPLODE(attributes) t AS attr_key, v
