SELECT product_id, store_id, COUNT(*) AS total_count
FROM inventory
WHERE store_id = 2
GROUP BY 1, 2
HAVING COUNT(*) >= 3
ORDER BY product_id DESC, store_id ASC
LIMIT 3
