SELECT last_restock
FROM inventory
WHERE store_id IN (1, 3, 4, 5)
INTERSECT
SELECT last_restock
FROM inventory
WHERE store_id IN (2, 3, 4)
