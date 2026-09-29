SELECT
    id,
    customer_id,
    created_at,
    tag
FROM carts
LATERAL VIEW OUTER EXPLODE(coupon_codes) li AS tag;
