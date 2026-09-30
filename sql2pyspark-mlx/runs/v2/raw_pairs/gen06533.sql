SELECT s.id, s.device, s.started_at, c.city, c.name, c.signup_date, CONCAT(c.name, ' ', c.email) AS name_email FROM sessions s LEFT OUTER JOIN customers c ON s.customer_id = c.id
