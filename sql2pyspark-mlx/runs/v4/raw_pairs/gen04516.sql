SELECT s.device, SUM(CASE WHEN we.page = '/search' THEN 1 ELSE 0 END) AS search_count, SUM(CASE WHEN we.event_type = 'search' THEN we.duration_sec ELSE 0 END) AS search_duration_sec
FROM web_events we
RIGHT JOIN sessions s ON we.session_id = s.id
INNER JOIN customers c ON s.customer_id = c.id
GROUP BY s.device
