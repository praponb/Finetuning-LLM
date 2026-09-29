SELECT
    o.id,
    o.channel,
    o.amount,
    o.status
FROM orders o
WHERE YEAR(o.order_date) >= 2025 AND NOT EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id AND p.amount <= 1060)
ORDER BY channel;
