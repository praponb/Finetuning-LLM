SELECT c.id, c.signup_date, c.city FROM customers c WHERE NOT EXISTS (SELECT 1 FROM subscriptions s WHERE s.customer_id = c.id AND s.monthly_fee <= 120)
