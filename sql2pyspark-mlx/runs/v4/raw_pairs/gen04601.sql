SELECT o.status, MAX(p.paid_at) last_paid_at
FROM payments p
LEFT OUTER JOIN orders o ON p.order_id = o.id
GROUP BY o.status
ORDER BY status
LIMIT 20;
