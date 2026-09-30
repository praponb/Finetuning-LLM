WITH order_stats AS (
    SELECT order_id, MAX(quantity) - MIN(quantity) AS quantity_range
    FROM order_items
    GROUP BY order_id
)
SELECT
    orders.id,
    orders.order_date,
    orders.store_id,
    order_stats.quantity_range
FROM orders
JOIN order_stats ON orders.id = order_stats.order_id
ORDER BY 4
