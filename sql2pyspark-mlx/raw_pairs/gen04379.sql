SELECT last_restock, store_id, quantity
FROM inventory
WHERE NOT (store_id IN (1, 2, 4) OR store_id IN (3, 4, 5, 6))
