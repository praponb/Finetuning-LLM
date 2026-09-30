SELECT s.device, s.country, COUNT(DISTINCT we.duration_sec) AS unique_event_types
FROM web_events we
RIGHT JOIN sessions s ON s.id = we.session_id
RIGHT JOIN customers c ON s.customer_id = c.id
WHERE s.customer_id IS NULL
GROUP BY s.device, s.country
ORDER BY s.country DESC;
