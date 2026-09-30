SELECT MIN(last_restock) AS first_last_restock
FROM inventory
WHERE store_id IN (4, 6) AND quantity BETWEEN 134 AND 229
ORDER BY 1
