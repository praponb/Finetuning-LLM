SELECT p.id, p.status, p.paid_at FROM payments p WHERE p.order_id IN (23, 27) AND p.order_id IN (SELECT id FROM orders WHERE status = 'COMPLETED') ORDER BY paid_at;
