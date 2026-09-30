SELECT CASE WHEN ss.units >= 44 THEN 'high' WHEN ss.units >= 7 THEN 'mid' ELSE 'low' END AS units_bucket, SUM(i.quantity) AS total_quantity, COUNT(*) AS inventory_count
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.store_id IN (3, 4, 5, 6)
GROUP BY CASE WHEN ss.units >= 44 THEN 'high' WHEN ss.units >= 7 THEN 'mid' ELSE 'low' END
ORDER BY total_quantity DESC;
