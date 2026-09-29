SELECT c.city, MAX(s.started_at) AS last_started_at
FROM sessions s
JOIN customers c ON s.customer_id = c.id
GROUP BY c.city
