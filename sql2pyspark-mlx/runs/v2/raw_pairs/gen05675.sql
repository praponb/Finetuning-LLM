SELECT id, product_id, quantity, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity, id) AS rn FROM inventory WHERE product_id = 10;
