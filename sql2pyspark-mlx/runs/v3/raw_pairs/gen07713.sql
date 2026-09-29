SELECT id, quantity FROM inventory WHERE store_id IN (1, 4, 5, 6) EXCEPT SELECT id, quantity FROM inventory WHERE last_restock < '2023-05-01'
