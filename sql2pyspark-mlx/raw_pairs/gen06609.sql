SELECT id, last_restock FROM inventory WHERE store_id = 6 EXCEPT SELECT id, last_restock FROM inventory WHERE last_restock < '2024-10-01';
