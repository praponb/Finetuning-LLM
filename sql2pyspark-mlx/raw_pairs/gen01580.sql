SELECT id, customer_id, created_at, coupon_codes, elem.qty, elem.promo FROM carts LATERAL VIEW EXPLODE(items) x AS elem;
