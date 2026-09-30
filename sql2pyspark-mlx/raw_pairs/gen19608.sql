SELECT s.id, s.started_at, s.browser, c.age, c.name, COALESCE(s.country, 'unassigned') AS country_filled FROM sessions s LEFT JOIN customers c ON s.customer_id = c.id ORDER BY s.id DESC LIMIT 3;
