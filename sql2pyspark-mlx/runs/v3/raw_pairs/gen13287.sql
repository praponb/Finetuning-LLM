SELECT s.country, s.device, s.started_at, c.signup_date, c.age FROM sessions s INNER JOIN customers c ON s.customer_id = c.id
