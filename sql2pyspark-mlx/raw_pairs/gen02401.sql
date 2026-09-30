SELECT s.id, s.status
FROM subscriptions s
WHERE s.customer_id NOT IN (SELECT id FROM customers WHERE city = 'Singapore')
ORDER BY s.id DESC, s.status;
