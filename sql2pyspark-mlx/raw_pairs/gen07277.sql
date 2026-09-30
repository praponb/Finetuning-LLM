SELECT id, store_id, last_restock
FROM inventory
WHERE last_restock > '2025-05-17'
UNION
SELECT id, store_id, last_restock
FROM inventory
WHERE quantity != 60;
