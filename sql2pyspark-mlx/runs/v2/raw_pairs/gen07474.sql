SELECT o.id, o.customer_id, o.status FROM orders o WHERE o.customer_id IN (SELECT id FROM customers WHERE segment IN ('SMB', 'Consumer')) ORDER BY o.status DESC;
