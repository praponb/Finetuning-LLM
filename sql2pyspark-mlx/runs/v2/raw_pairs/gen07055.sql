SELECT c.id, c.credit_limit, c.city, c.signup_date FROM customers c WHERE c.city = 'Berlin' AND NOT EXISTS (SELECT 1 FROM sessions s WHERE s.customer_id = c.id);
