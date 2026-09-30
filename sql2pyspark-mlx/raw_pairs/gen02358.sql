SELECT id, last_restock, store_id
FROM inventory
WHERE last_restock >= (SELECT MAX(last_restock) FROM inventory)
ORDER BY store_id DESC, id
LIMIT 5
