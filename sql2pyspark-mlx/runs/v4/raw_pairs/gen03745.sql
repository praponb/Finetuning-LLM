SELECT id, store_id, quantity, MIN(quantity) OVER (PARTITION BY store_id) AS store_id_min_quantity FROM inventory WHERE store_id IN (1, 2, 4, 5) ORDER BY store_id DESC
