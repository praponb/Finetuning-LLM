SELECT ss.product_id, COUNT(i.quantity) AS quantity_count, COUNT(*) AS num_inventory
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.product_id = 10
GROUP BY ss.product_id
HAVING COUNT(i.quantity) > 2
ORDER BY ss.product_id DESC
LIMIT 20
