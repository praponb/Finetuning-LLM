SELECT CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 4 THEN 'mid' ELSE 'low' END AS quantity_bucket, MIN(quantity) AS min_quantity
FROM order_items
WHERE order_id = 15
GROUP BY 1
HAVING MIN(quantity) < 2.9
ORDER BY quantity_bucket DESC;
