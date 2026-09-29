SELECT c.id, c.signup_date, c.city FROM customers c WHERE NOT EXISTS (SELECT 1 FROM sessions s WHERE s.customer_id = c.id);
