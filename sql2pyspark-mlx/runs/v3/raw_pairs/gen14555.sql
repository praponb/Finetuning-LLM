SELECT ss.store_id, AVG(i.quantity) avg_quantity
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.revenue BETWEEN 3000 AND 3800
GROUP BY ss.store_id
ORDER BY 2 DESC, store_id
LIMIT 10
