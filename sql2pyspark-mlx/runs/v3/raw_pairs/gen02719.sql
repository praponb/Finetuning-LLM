SELECT CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 4 THEN 'mid' ELSE 'low' END AS quantity_bucket, MAX(quantity) - MIN(quantity) AS quantity_range, ROUND(STDDEV(quantity), 2) AS stddev_quantity
FROM order_items
WHERE order_id IN (31, 36)
GROUP BY 1
ORDER BY quantity_range DESC;
