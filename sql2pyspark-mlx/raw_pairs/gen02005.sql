WITH flattened AS (SELECT id, created_at, element FROM carts LATERAL VIEW EXPLODE(coupon_codes) e AS element) SELECT created_at, element FROM flattened ORDER BY element ASC, created_at ASC
