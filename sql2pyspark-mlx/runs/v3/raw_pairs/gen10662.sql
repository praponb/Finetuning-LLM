SELECT c.country, SUM(CASE WHEN s.device = 'desktop' THEN 1 ELSE 0 END) AS desktop_count, MIN(s.started_at) AS min_started_at
FROM sessions s
JOIN customers c ON s.customer_id = c.id
WHERE c.name IS NOT NULL
GROUP BY c.country
ORDER BY c.country DESC
