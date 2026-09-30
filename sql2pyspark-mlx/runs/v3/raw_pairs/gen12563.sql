SELECT c.id, c.age
FROM customers c
WHERE EXISTS (SELECT 1 FROM subscriptions s WHERE s.customer_id = c.id)
ORDER BY c.id DESC
