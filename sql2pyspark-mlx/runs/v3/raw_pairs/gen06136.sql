SELECT id, product_id, last_restock, quantity, FIRST_VALUE(quantity) OVER (PARTITION BY product_id ORDER BY last_restock, id) AS first_quantity FROM inventory WHERE store_id IN (1, 2, 3, 5);
