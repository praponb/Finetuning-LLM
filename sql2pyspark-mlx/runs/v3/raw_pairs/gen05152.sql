SELECT p.id, p.status FROM payments p WHERE p.order_id IN (SELECT id FROM orders WHERE status IN ('COMPLETED', 'CANCELLED')) ORDER BY status DESC
