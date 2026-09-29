WITH flattened AS (SELECT id, EXPLODE(coupon_codes) AS tag FROM carts) SELECT id, tag FROM flattened ORDER BY id DESC NULLS FIRST, tag DESC;
