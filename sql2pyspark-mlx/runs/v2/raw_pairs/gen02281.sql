SELECT
    order_id,
    CASE WHEN unit_price >= 420 THEN 'high' WHEN unit_price >= 80 THEN 'mid' ELSE 'low' END unit_price_bucket,
    MAX(quantity) max_quantity,
    MAX(unit_price) - MIN(unit_price) unit_price_range,
    AVG(unit_price) avg_unit_price
FROM order_items
GROUP BY order_id, unit_price_bucket
HAVING unit_price_range > 52
