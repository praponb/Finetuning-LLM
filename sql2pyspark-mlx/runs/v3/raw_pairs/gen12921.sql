SELECT s.id, s.country, s.started_at, s.device, c.segment, c.city FROM sessions s LEFT JOIN customers c ON s.customer_id = c.id;
