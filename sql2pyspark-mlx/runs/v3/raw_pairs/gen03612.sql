SELECT c.id, c.signup_date FROM customers c WHERE NOT EXISTS (SELECT 1 FROM subscriptions s WHERE s.customer_id = c.id AND s.start_date < '2024-05-01')
