SELECT
    id,
    created_at,
    coupon_codes,
    k,
    attr_value
FROM carts
LATERAL VIEW EXPLODE(attributes) li AS k, attr_value;
