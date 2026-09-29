SELECT ss.product_id, SUM(i.quantity) AS total_quantity, MAX(i.last_restock) AS latest_last_restock
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.product_id = 4
GROUP BY ss.product_id
ORDER BY latest_last_restock, product_id
LIMIT 5
