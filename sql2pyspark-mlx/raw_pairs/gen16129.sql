SELECT id, customer_id, created_at, k, v FROM carts LATERAL VIEW EXPLODE(attributes) li AS k, v
