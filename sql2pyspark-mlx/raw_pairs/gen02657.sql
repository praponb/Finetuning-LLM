WITH lines AS (SELECT id, customer_id, attr_key, attr_value FROM carts LATERAL VIEW EXPLODE(attributes) li AS attr_key, attr_value) SELECT id, customer_id, attr_key FROM lines ORDER BY id;
