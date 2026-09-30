SELECT id, product_id, last_restock, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock, id) AS rn FROM inventory WHERE product_id IN (1, 3, 4);
