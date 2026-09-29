SELECT
    CASE WHEN ss.revenue >= 4000 THEN 'high' WHEN ss.revenue >= 900 THEN 'mid' ELSE 'low' END AS revenue_bucket,
    ss.product_id,
    COUNT(DISTINCT i.quantity) AS unique_products,
    MAX(i.last_restock) AS latest_last_restock
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.product_id IN (1, 6, 8, 9)
GROUP BY CASE WHEN ss.revenue >= 4000 THEN 'high' WHEN ss.revenue >= 900 THEN 'mid' ELSE 'low' END, ss.product_id
HAVING COUNT(DISTINCT i.quantity) > 1
ORDER BY product_id DESC, revenue_bucket;
