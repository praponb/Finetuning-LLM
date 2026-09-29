WITH order_stats AS (
    SELECT order_id, MAX(quantity) - MIN(quantity) AS quantity_range
    FROM order_items
    GROUP BY order_id
)
SELECT o.id, o.channel, os.quantity_range
FROM orders o
LEFT OUTER JOIN order_stats os ON o.id = os.order_id
WHERE os.quantity_range >= 50
ORDER BY os.quantity_range
