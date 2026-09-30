SELECT
    o.id,
    o.status,
    COUNT(p.id) AS payment_count,
    MAX(p.paid_at) AS latest_paid_at,
    MIN(p.paid_at) AS first_paid_at
FROM orders o
LEFT OUTER JOIN payments p ON p.order_id = o.id
GROUP BY o.id, o.status
