SELECT s.id, s.plan
FROM subscriptions s
WHERE s.start_date > '2023-09-01' AND s.customer_id NOT IN (SELECT id FROM customers WHERE email IS NOT NULL)
