SELECT s.id, s.customer_id, s.device, s.browser, s.started_at, s.country, p.credit_limit, p.name FROM sessions s LEFT JOIN customers p ON s.customer_id = p.id;
