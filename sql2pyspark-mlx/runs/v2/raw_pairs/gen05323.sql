SELECT s.id, s.start_date
FROM subscriptions s
WHERE s.customer_id IN (SELECT id FROM customers WHERE country = 'UK')
ORDER BY s.start_date
