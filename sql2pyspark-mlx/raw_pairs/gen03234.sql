WITH flattened AS (SELECT id, coupon_codes, k, v FROM carts LATERAL VIEW OUTER EXPLODE(attributes) t AS k, v) SELECT id, coupon_codes, k FROM flattened ORDER BY id DESC;
