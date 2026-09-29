SELECT p.id, p.paid_at
FROM payments p
WHERE p.order_id NOT IN (SELECT id FROM orders WHERE status NOT IN ('COMPLETED', 'PENDING', 'RETURNED'));
