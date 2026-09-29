SELECT
    CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 4 THEN 'mid' ELSE 'low' END quantity_bucket,
    COUNT(unit_price) unit_price_count,
    ROUND(AVG(quantity), 2) avg_quantity,
    AVG(unit_price) avg_unit_price
FROM order_items
WHERE order_id IN (25, 40)
GROUP BY quantity_bucket
HAVING AVG(unit_price) < 340
ORDER BY quantity_bucket;
