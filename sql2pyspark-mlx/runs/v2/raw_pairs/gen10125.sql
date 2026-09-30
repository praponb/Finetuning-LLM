SELECT c.is_active, c.country, SUM(CASE WHEN s.device = 'desktop' THEN 1 ELSE 0 END) AS desktop_count FROM sessions s INNER JOIN customers c ON s.customer_id = c.id GROUP BY c.is_active, c.country
