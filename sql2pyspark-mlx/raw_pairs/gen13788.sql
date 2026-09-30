SELECT s.id, s.device, s.started_at, c.segment, c.email, COALESCE(s.country, 'N/A') AS country_filled FROM sessions s LEFT OUTER JOIN customers c ON s.customer_id = c.id ORDER BY 2, id LIMIT 50;
