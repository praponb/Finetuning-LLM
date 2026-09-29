SELECT id, customer_id, EXPLODE(coupon_codes) AS value FROM carts;
