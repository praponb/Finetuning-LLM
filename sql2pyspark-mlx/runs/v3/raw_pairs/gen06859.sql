SELECT CASE WHEN ss.revenue >= 3900 THEN 'high' WHEN ss.revenue >= 1000 THEN 'mid' ELSE 'low' END AS revenue_bucket, MAX(i.quantity) AS max_quantity, AVG(i.quantity) AS avg_quantity
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.product_id IN (3, 5, 6, 10)
GROUP BY CASE WHEN ss.revenue >= 3900 THEN 'high' WHEN ss.revenue >= 1000 THEN 'mid' ELSE 'low' END
ORDER BY 2 DESC, revenue_bucket
LIMIT 3
