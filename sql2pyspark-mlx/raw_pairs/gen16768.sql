SELECT id, element
FROM carts
LATERAL VIEW OUTER EXPLODE(coupon_codes) e AS element
