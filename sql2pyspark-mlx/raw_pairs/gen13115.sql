WITH lines AS (SELECT id, customer_id, EXPLODE_OUTER(coupon_codes) AS element FROM carts) SELECT id, customer_id FROM lines ORDER BY id DESC NULLS FIRST
