SELECT store_id, quantity
FROM inventory
WHERE store_id IN (4, 6)
UNION ALL
SELECT store_id, quantity
FROM inventory
WHERE YEAR(last_restock) >= 2025
ORDER BY quantity
