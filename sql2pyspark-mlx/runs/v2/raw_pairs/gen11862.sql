SELECT
    o.id,
    o.amount,
    t.payment_count,
    t.min_paid_at
FROM orders o
INNER JOIN (SELECT order_id, COUNT(*) AS payment_count, MIN(paid_at) AS min_paid_at
    FROM payments
    GROUP BY order_id) t ON o.id = t.order_id
