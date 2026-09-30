SELECT s.customer_id, s.country, s.device, c.is_active, c.email, c.signup_date FROM sessions s FULL OUTER JOIN customers c ON s.customer_id = c.id WHERE NOT c.is_active OR c.segment != 'Consumer'
