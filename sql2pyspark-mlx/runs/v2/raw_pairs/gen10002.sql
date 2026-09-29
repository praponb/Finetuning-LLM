WITH order_summary AS (
    SELECT order_id, SUM(quantity * unit_price) AS revenue
    FROM order_items
    GROUP BY order_id
)
SELECT o.id, o.amount, os.revenue
FROM orders o
LEFT JOIN order_summary os ON o.id = os.order_id;
