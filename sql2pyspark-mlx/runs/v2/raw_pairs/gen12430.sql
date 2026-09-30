SELECT a.id, a.balance FROM accounts a WHERE a.customer_id IN (SELECT id FROM customers WHERE city NOT IN ('Berlin', 'Tokyo', 'Singapore')) ORDER BY a.id LIMIT 20
