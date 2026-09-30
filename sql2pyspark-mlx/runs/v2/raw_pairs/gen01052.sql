SELECT
    o.id,
    o.status,
    COUNT(p.id) AS payment_count,
    MIN(p.amount) AS min_amount,
    MAX(p.amount) - MIN(p.amount) AS amount_range
FROM orders o
LEFT JOIN payments p ON o.id = p.order_id
GROUP BY o.id, o.status
ORDER BY min_amount DESC, id, status
LIMIT 5;
