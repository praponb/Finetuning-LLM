WITH OrderStats AS (
    SELECT order_id, MAX(delivered_date) AS last_delivered_date
    FROM shipments
    WHERE order_id IS NULL
    GROUP BY order_id
)
SELECT orders.order_date, orders.customer_id, OrderStats.last_delivered_date
FROM orders
JOIN OrderStats ON orders.id = OrderStats.order_id
ORDER BY order_date ASC;
