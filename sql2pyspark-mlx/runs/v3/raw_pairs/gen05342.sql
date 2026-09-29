SELECT p.id, p.status, p.method FROM payments p WHERE p.method IN ('card', 'cash') AND p.order_id NOT IN (SELECT id FROM orders WHERE status IN ('PENDING', 'RETURNED')) ORDER BY p.method DESC
