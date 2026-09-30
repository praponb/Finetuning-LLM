SELECT id, store_id, quantity, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity DESC, id) AS row_num FROM inventory WHERE store_id IN (1, 2, 3, 6);
