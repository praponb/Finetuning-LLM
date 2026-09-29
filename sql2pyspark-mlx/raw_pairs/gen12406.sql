WITH lines AS (SELECT id, attr_key, attr_value FROM carts LATERAL VIEW OUTER EXPLODE(attributes) t AS attr_key, attr_value) SELECT id FROM lines ORDER BY id DESC;
