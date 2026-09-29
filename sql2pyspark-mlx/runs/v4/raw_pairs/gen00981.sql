SELECT id, value
FROM carts
LATERAL VIEW EXPLODE(coupon_codes) li AS value
