SELECT id, customer_id, created_at, coupon_codes, key, value FROM carts LATERAL VIEW EXPLODE(attributes) e AS key, value;
