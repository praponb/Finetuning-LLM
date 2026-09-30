WITH order_stats AS (
    SELECT order_id, COUNT(quantity) AS quantity_count
    FROM order_items
    GROUP BY order_id
)
SELECT o.id, o.customer_id, os.quantity_count
FROM orders o
INNER JOIN order_stats os ON o.id = os.order_id
WHERE os.quantity_count >= 9
ORDER BY id DESC, os.quantity_count DESC
