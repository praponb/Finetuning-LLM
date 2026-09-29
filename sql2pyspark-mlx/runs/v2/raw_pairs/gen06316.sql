SELECT o.id, o.channel
FROM orders o
WHERE NOT EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id AND p.paid_at < '2025-07-01')
