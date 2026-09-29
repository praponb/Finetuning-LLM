SELECT store_id, quantity
FROM inventory
WHERE store_id IN (1, 4, 6)
UNION
SELECT store_id, quantity
FROM inventory
WHERE last_restock < '2025-07-01';
