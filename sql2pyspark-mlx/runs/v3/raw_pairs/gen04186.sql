WITH payments_by_order AS (
    SELECT order_id, COUNT(DISTINCT amount) AS unique_orders
    FROM payments
    WHERE status = 'REFUNDED'
    GROUP BY order_id
)
SELECT
    orders.id,
    orders.customer_id,
    orders.channel,
    payments_by_order.unique_orders
FROM orders
INNER JOIN payments_by_order ON orders.id = payments_by_order.order_id
WHERE payments_by_order.unique_orders > 6
ORDER BY id, unique_orders DESC
