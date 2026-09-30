SELECT id, EXPLODE_OUTER(coupon_codes) AS value FROM carts
