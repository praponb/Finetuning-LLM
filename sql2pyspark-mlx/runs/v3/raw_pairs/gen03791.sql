SELECT s.country, COUNT(we.id) AS web_event_count
FROM sessions s
INNER JOIN web_events we ON we.session_id = s.id
GROUP BY s.country
ORDER BY s.country ASC;
