SELECT CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 4 THEN 'mid' ELSE 'low' END AS quantity_bucket, ROUND(STDDEV(unit_price), 2) AS stddev_unit_price
FROM order_items
GROUP BY CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 4 THEN 'mid' ELSE 'low' END
HAVING MIN(unit_price) > 180
ORDER BY stddev_unit_price, quantity_bucket
LIMIT 3
