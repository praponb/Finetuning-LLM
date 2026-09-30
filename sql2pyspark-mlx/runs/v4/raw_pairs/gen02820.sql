SELECT s.id, s.customer_id
FROM subscriptions s
WHERE s.customer_id NOT IN (SELECT id FROM customers WHERE segment <> 'Consumer')
