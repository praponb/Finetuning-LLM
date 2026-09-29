SELECT s.id, s.country, s.started_at, s.device, c.name, c.country AS customer_country, TO_DATE(s.started_at) AS started_at_date FROM sessions s JOIN customers c ON s.customer_id = c.id;
