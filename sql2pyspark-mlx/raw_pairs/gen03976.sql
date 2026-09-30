WITH exploded AS (SELECT id, customer_id, attr_key, value FROM carts LATERAL VIEW EXPLODE(attributes) e AS attr_key, value) SELECT id, attr_key FROM exploded ORDER BY attr_key, id DESC
