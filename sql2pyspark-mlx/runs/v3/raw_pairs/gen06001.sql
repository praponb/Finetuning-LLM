SELECT id, store_id, quantity FROM inventory WHERE quantity != 182 UNION SELECT id, store_id, quantity FROM inventory WHERE store_id IN (1, 5);
