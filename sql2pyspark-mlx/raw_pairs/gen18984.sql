WITH flattened AS (SELECT id, created_at, coupon_codes, key, v FROM carts LATERAL VIEW OUTER EXPLODE(attributes) e AS key, v) SELECT id, coupon_codes, v FROM flattened
