SELECT s.id, s.country, s.device, c.signup_date, c.credit_limit FROM sessions s INNER JOIN customers c ON c.id = s.customer_id ORDER BY c.credit_limit, id LIMIT 3
