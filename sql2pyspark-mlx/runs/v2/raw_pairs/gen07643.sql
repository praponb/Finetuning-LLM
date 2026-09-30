SELECT CASE WHEN unit_price >= 340 THEN 'high' WHEN unit_price >= 200 THEN 'mid' ELSE 'low' END AS unit_price_bucket, MIN(quantity) AS min_quantity, COUNT(DISTINCT quantity) AS unique_orders
FROM order_items
WHERE quantity IS NOT NULL
GROUP BY CASE WHEN unit_price >= 340 THEN 'high' WHEN unit_price >= 200 THEN 'mid' ELSE 'low' END
HAVING MIN(quantity) > 6.8
ORDER BY min_quantity DESC;
