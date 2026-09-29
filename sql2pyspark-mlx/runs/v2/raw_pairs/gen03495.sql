WITH order_stats AS (
    SELECT order_id, COUNT(unit_price) AS unit_price_count
    FROM order_items
    GROUP BY order_id
)
SELECT o.channel, o.status, os.unit_price_count
FROM orders o
INNER JOIN order_stats os ON o.id = os.order_id
WHERE os.unit_price_count > 9
