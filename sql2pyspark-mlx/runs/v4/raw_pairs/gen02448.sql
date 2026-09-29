SELECT
    id,
    customer_id,
    coupon_codes,
    k,
    attr_value
FROM carts
LATERAL VIEW OUTER EXPLODE(attributes) li AS k, attr_value
