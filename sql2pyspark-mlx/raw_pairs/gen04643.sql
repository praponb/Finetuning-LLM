WITH lines AS (SELECT id, customer_id, created_at, element FROM carts LATERAL VIEW EXPLODE(coupon_codes) li AS element) SELECT id, created_at, element FROM lines ORDER BY element DESC, id DESC
