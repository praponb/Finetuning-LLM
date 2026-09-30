SELECT id, customer_id, element FROM carts LATERAL VIEW OUTER EXPLODE(coupon_codes) li AS element;
