SELECT id, coupon_codes, attr_key, v FROM carts LATERAL VIEW OUTER EXPLODE(attributes) li AS attr_key, v;
