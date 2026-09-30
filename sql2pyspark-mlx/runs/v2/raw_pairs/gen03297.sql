SELECT id, store_id, quantity, NTILE(3) OVER (PARTITION BY store_id ORDER BY quantity DESC, id) bucket FROM inventory WHERE store_id IN (1, 3, 4, 5);
