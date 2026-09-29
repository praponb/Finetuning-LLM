WITH order_summary AS (
    SELECT order_id, ROUND(AVG(amount), 2) AS avg_amount
    FROM payments
    GROUP BY order_id
)
SELECT orders.id, orders.channel, order_summary.avg_amount
FROM orders
INNER JOIN order_summary ON orders.id = order_summary.order_id
ORDER BY orders.id DESC;
