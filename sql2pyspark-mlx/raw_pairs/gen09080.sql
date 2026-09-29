WITH lines AS (SELECT id, created_at, coupon_codes, k, value FROM carts LATERAL VIEW EXPLODE(attributes) e AS k, value) SELECT id, created_at, k, value FROM lines
