SELECT id, customer_id, k, v FROM carts LATERAL VIEW EXPLODE(attributes) li AS k, v
