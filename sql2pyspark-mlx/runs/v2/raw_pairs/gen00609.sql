SELECT
    o.id,
    o.channel,
    COUNT(p.id) AS payment_count,
    MAX(p.paid_at) AS last_paid_at,
    SUM(p.amount) AS total_amount
FROM orders o
JOIN payments p ON o.id = p.order_id
WHERE o.status = 'CANCELLED'
GROUP BY o.id, o.channel
