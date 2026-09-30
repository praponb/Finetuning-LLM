SELECT id, customer_id, created_at, attr_key, value FROM carts LATERAL VIEW OUTER EXPLODE(attributes) t AS attr_key, value;
