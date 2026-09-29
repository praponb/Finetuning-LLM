SELECT o.id, o.channel
FROM orders o
WHERE o.status IN ('RETURNED', 'COMPLETED') AND NOT EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id)
ORDER BY 1;
