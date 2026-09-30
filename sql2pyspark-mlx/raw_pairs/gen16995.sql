SELECT s.country, COUNT(we.id) web_event_count, SUM(we.duration_sec) total_duration_sec
FROM sessions s
INNER JOIN web_events we ON s.id = we.session_id
GROUP BY s.country
ORDER BY s.country DESC
LIMIT 3;
