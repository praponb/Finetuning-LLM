SELECT p.id, p.amount, p.method
FROM payments p
WHERE p.order_id IN (SELECT id FROM orders WHERE status = 'CANCELLED');
