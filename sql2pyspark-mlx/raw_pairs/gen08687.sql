SELECT s.id, COUNT(we.id) AS web_event_count
FROM sessions s
LEFT JOIN web_events we ON we.session_id = s.id
GROUP BY s.id
HAVING COUNT(we.id) >= 5
ORDER BY id DESC, web_event_count DESC
