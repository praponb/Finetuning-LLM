SELECT id, created_at, key, v FROM carts LATERAL VIEW OUTER EXPLODE(attributes) x AS key, v
