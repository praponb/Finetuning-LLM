SELECT p.id, p.paid_at, p.status
FROM payments p
WHERE p.status != 'REFUNDED' AND p.order_id NOT IN (SELECT id FROM orders WHERE status = 'RETURNED');
