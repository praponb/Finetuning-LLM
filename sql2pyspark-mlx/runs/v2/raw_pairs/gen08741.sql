SELECT c.country, COUNT(*) total_count, MAX(s.started_at) last_started_at
FROM sessions s
FULL OUTER JOIN customers c ON s.customer_id = c.id
GROUP BY c.country
ORDER BY last_started_at ASC
