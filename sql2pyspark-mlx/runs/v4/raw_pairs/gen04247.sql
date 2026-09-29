SELECT CASE WHEN unit_price >= 410 THEN 'high' WHEN unit_price >= 110 THEN 'mid' ELSE 'low' END unit_price_bucket, ROUND(STDDEV(quantity), 2) stddev_quantity
FROM order_items
GROUP BY 1
HAVING MIN(unit_price) > 220
ORDER BY stddev_quantity;
