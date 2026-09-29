SELECT s.id, s.monthly_fee FROM subscriptions s WHERE s.customer_id IN (32, 35) AND s.customer_id IN (SELECT id FROM customers WHERE city NOT IN ('Bangkok', 'Singapore')) ORDER BY id ASC LIMIT 3
