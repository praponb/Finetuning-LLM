SELECT
    o.status,
    o.channel,
    ROUND(AVG(p.amount), 2) AS avg_amount,
    COUNT(p.amount) AS amount_count
FROM payments p
JOIN orders o ON p.order_id = o.id
GROUP BY o.status, o.channel
ORDER BY status DESC;
