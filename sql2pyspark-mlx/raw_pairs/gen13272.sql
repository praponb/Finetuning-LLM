SELECT CASE WHEN unit_price >= 420 THEN 'high' WHEN unit_price >= 160 THEN 'mid' ELSE 'low' END unit_price_bucket, COUNT(unit_price) unit_price_count
FROM order_items
WHERE unit_price < 400 AND quantity <= 6
GROUP BY 1
ORDER BY unit_price_bucket DESC
