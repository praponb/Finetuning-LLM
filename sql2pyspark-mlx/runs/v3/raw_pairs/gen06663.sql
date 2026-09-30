SELECT s.id, s.status FROM subscriptions s WHERE s.plan = 'basic' AND s.customer_id IN (SELECT id FROM customers WHERE city NOT IN ('Tokyo', 'Berlin', 'Singapore')) ORDER BY s.id DESC LIMIT 5
