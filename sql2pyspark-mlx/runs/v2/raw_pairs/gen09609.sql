SELECT ss.store_id, SUM(i.quantity) total_quantity
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.last_restock > '2023-11-01'
GROUP BY ss.store_id
HAVING SUM(i.quantity) > 1320
ORDER BY store_id DESC
LIMIT 20
