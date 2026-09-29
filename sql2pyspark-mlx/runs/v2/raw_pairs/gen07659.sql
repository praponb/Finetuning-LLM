SELECT product_id, MAX(last_restock) AS latest_last_restock, SUM(quantity) AS total_quantity
FROM inventory
WHERE MONTH(last_restock) IN (5, 8, 11)
GROUP BY product_id
HAVING COUNT(quantity) >= 3
ORDER BY product_id DESC;
