WITH order_totals AS (
    SELECT order_id, AVG(amount) AS avg_amount
    FROM payments
    GROUP BY order_id
)
SELECT o.customer_id, o.order_date, ot.avg_amount
FROM orders o
LEFT OUTER JOIN order_totals ot ON o.id = ot.order_id;
