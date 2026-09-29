SELECT product_id, MAX(last_restock) latest_last_restock
FROM inventory
WHERE product_id IN (5, 8)
GROUP BY product_id
ORDER BY 1 DESC
LIMIT 5
