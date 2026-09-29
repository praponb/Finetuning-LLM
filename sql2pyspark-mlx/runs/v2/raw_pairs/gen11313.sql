SELECT order_id, unit_price_bucket, revenue
FROM (
    SELECT order_id, CASE WHEN unit_price >= 360 THEN 'high' WHEN unit_price >= 130 THEN 'mid' ELSE 'low' END unit_price_bucket, SUM(unit_price * quantity) revenue
    FROM order_items
    GROUP BY order_id, CASE WHEN unit_price >= 360 THEN 'high' WHEN unit_price >= 130 THEN 'mid' ELSE 'low' END
) x
WHERE revenue < 30
ORDER BY revenue ASC, order_id ASC, unit_price_bucket ASC
LIMIT 10;
