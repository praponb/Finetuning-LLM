SELECT c.id, c.signup_date FROM customers c WHERE NOT EXISTS (SELECT 1 FROM subscriptions s WHERE s.customer_id = c.id AND s.end_date < '2023-09-01');
