SELECT id, created_at, coupon_codes, key, value FROM carts LATERAL VIEW EXPLODE(attributes) li AS key, value
