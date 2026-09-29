WITH OrderSummary AS (
    SELECT order_id, SUM(quantity * unit_price) AS revenue
    FROM order_items
    GROUP BY order_id
)
SELECT o.id, o.amount, O.revenue
FROM orders o
LEFT JOIN OrderSummary O ON o.id = O.order_id
