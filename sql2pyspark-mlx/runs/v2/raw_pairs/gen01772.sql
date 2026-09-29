SELECT c.city, AVG(we.duration_sec) AS avg_duration_sec, SUM(we.duration_sec) AS total_duration_sec
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
INNER JOIN customers c ON c.id = s.customer_id
WHERE s.device = 'desktop'
GROUP BY c.city;
