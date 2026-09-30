SELECT id, customer_id, created_at, coupon_codes, key, v FROM carts LATERAL VIEW EXPLODE(attributes) li AS key, v
