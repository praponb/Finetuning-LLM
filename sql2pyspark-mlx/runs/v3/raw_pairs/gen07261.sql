SELECT s.country, s.started_at, s.device, c.name FROM sessions s LEFT JOIN customers c ON s.customer_id = c.id WHERE s.started_at < '2024-04-01' AND c.city IN ('Chiang Mai', 'Berlin')
