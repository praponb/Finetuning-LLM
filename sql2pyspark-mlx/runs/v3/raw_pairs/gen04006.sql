WITH order_stats AS (
    SELECT order_id, ROUND((100 * SUM(CASE WHEN carrier = 'Flash' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_flash, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(carrier))) AS carrier_values
    FROM shipments
    GROUP BY order_id
)
SELECT
    orders.amount,
    orders.order_date,
    order_stats.pct_flash,
    order_stats.carrier_values
FROM orders
INNER JOIN order_stats ON orders.id = order_stats.order_id
WHERE order_stats.pct_flash > 58
ORDER BY orders.amount;
