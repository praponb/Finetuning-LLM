SELECT s.device, s.country, c.credit_limit, c.city FROM sessions s JOIN customers c ON s.customer_id = c.id
