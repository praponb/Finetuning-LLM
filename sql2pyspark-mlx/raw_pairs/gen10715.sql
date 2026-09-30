WITH order_totals AS (
    SELECT order_id, MIN(paid_at) first_paid_at
    FROM payments
    GROUP BY order_id
)
SELECT o.id, o.customer_id, ot.first_paid_at
FROM orders o
LEFT JOIN order_totals ot ON o.id = ot.order_id;
