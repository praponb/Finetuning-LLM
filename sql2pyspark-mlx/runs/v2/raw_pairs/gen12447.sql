SELECT s.country, c.is_active, c.name FROM sessions s LEFT JOIN customers c ON s.customer_id = c.id ORDER BY name
