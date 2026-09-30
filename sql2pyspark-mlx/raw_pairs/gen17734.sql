SELECT
    o.status,
    o.amount,
    p.status AS payment_status,
    p.method,
    p.paid_at
FROM orders o
INNER JOIN payments p ON o.id = p.order_id
ORDER BY p.method, 3
