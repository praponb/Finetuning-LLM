SELECT p.id, p.order_id FROM payments p WHERE p.order_id IN (SELECT id FROM orders WHERE status IN ('SHIPPED', 'PENDING', 'RETURNED')) ORDER BY order_id DESC, id LIMIT 10;
