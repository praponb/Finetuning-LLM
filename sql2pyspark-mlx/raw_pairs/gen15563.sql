SELECT store_id
FROM inventory
WHERE quantity IS NULL AND store_id IN (1, 2, 4)
ORDER BY store_id DESC
