SELECT id, last_restock FROM inventory WHERE quantity > 84 EXCEPT SELECT id, last_restock FROM inventory WHERE store_id = 5 ORDER BY id
