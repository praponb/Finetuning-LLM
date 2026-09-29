SELECT id, code FROM carts LATERAL VIEW OUTER EXPLODE(coupon_codes) li AS code
