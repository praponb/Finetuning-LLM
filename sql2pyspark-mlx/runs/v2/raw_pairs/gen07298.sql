SELECT id, product_id, quantity, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS rn FROM inventory ORDER BY quantity DESC;
