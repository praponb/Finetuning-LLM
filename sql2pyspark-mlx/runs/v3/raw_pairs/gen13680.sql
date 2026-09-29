WITH order_stats AS (
    SELECT order_id, ROUND(STDDEV(quantity), 2) stddev_quantity, COUNT(DISTINCT unit_price) unique_orders
    FROM order_items
    GROUP BY order_id
)
SELECT
    o.id,
    o.channel,
    o.amount,
    os.stddev_quantity,
    os.unique_orders
FROM orders o
LEFT JOIN order_stats os ON o.id = os.order_id
ORDER BY channel
