SELECT CASE WHEN unit_price >= 340 THEN 'high' WHEN unit_price >= 140 THEN 'mid' ELSE 'low' END unit_price_bucket, MAX(unit_price) - MIN(unit_price) unit_price_range, SUM(quantity) total_quantity
FROM order_items
WHERE product_id IS NULL AND (order_id IN (3, 15, 30) OR unit_price IS NULL)
GROUP BY CASE WHEN unit_price >= 340 THEN 'high' WHEN unit_price >= 140 THEN 'mid' ELSE 'low' END
ORDER BY unit_price_bucket ASC;
