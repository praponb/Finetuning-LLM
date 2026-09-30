SELECT last_restock
FROM inventory
WHERE store_id = 2
INTERSECT
SELECT opened_date
FROM stores
ORDER BY last_restock;
