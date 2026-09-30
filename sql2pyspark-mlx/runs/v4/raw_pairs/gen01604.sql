WITH order_stats AS (
    SELECT order_id, SUM(quantity) AS total_quantity
    FROM order_items
    GROUP BY order_id
)
SELECT orders.customer_id, orders.shipping_fee, order_stats.total_quantity
FROM orders
INNER JOIN order_stats ON orders.id = order_stats.order_id
ORDER BY total_quantity DESC
