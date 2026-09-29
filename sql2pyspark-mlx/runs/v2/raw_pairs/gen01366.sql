SELECT CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 4 THEN 'mid' ELSE 'low' END AS quantity_bucket, MAX(quantity) AS max_quantity, AVG(quantity) AS avg_quantity
FROM order_items
GROUP BY CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 4 THEN 'mid' ELSE 'low' END
HAVING MAX(quantity) > 4.9
ORDER BY avg_quantity
