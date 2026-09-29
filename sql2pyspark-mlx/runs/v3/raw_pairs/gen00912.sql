WITH order_totals AS (
    SELECT order_id, SUM(cost) AS total_cost
    FROM shipments
    GROUP BY order_id
)
SELECT o.order_date, o.customer_id, ot.total_cost
FROM orders o
JOIN order_totals ot ON o.id = ot.order_id
WHERE ot.total_cost > 530;
