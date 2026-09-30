SELECT id, store_id, quantity, MIN(quantity) OVER (PARTITION BY store_id) AS store_id_min_quantity FROM inventory
