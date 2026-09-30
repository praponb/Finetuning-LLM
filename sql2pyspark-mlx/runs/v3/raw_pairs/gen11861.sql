SELECT s.id, s.status, s.customer_id, s.plan FROM subscriptions s WHERE s.customer_id IN (SELECT id FROM customers WHERE signup_date >= '2024-03-01')
