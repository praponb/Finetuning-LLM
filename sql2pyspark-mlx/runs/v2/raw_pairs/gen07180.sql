SELECT id, product_id, store_id, quantity, MAX(quantity) OVER (PARTITION BY product_id, store_id) AS product_id_max_quantity FROM inventory WHERE store_id IN (2, 3, 4, 6);
