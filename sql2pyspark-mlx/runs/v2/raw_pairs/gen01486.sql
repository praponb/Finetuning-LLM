SELECT s.started_at, s.customer_id, s.device, c.city, c.signup_date, c.country FROM sessions s INNER JOIN customers c ON s.customer_id = c.id;
