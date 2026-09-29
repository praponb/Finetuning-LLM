SELECT CASE WHEN quantity >= 268 THEN 'high' WHEN quantity >= 61 THEN 'mid' ELSE 'low' END quantity_bucket, MAX(quantity) - MIN(quantity) quantity_range, AVG(quantity) avg_quantity
FROM inventory
WHERE store_id IN (1, 2, 4, 5)
GROUP BY quantity_bucket
ORDER BY avg_quantity DESC
