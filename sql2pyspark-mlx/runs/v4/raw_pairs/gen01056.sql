SELECT id, created_at, coupon_codes, k, v FROM carts LATERAL VIEW OUTER EXPLODE(attributes) li AS k, v;
