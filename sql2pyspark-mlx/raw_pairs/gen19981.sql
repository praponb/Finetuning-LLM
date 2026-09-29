SELECT store_id, ROUND(AVG(quantity), 2) AS avg_quantity, MIN(quantity) AS min_quantity
FROM inventory
WHERE store_id IN (2, 3, 4)
GROUP BY 1
ORDER BY store_id DESC, min_quantity ASC
