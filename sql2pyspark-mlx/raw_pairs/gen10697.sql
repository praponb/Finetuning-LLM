SELECT s.id, COUNT(we.id) AS web_event_count
FROM sessions s
INNER JOIN web_events we ON s.id = we.session_id
WHERE TO_DATE(s.started_at) >= '2024-03-21'
GROUP BY s.id
ORDER BY id DESC, web_event_count DESC;
