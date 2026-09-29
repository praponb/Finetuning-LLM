SELECT s.customer_id, HOUR(s.started_at) AS started_hour, SUM(CASE WHEN we.page = '/search' THEN we.duration_sec ELSE 0 END) AS search_duration_sec
FROM web_events we
INNER JOIN sessions s ON s.id = we.session_id
GROUP BY s.customer_id, HOUR(s.started_at)
HAVING SUM(CASE WHEN we.page = '/search' THEN we.duration_sec ELSE 0 END) >= 40
ORDER BY s.customer_id DESC
