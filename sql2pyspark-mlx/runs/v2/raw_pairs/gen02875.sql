WITH payments_by_order AS (
    SELECT order_id, MIN(paid_at) AS first_paid_at
    FROM payments
    GROUP BY order_id
)
SELECT o.id, o.customer_id, pbo.first_paid_at
FROM orders o
JOIN payments_by_order pbo ON o.id = pbo.order_id
ORDER BY first_paid_at, id
LIMIT 1;
