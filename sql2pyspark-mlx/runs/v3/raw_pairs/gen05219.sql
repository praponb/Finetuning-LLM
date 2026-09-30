SELECT s.id, s.device, COUNT(we.id) AS web_event_count
FROM sessions s
LEFT OUTER JOIN web_events we ON we.session_id = s.id
GROUP BY s.id, s.device
HAVING COUNT(we.id) > 3
ORDER BY id DESC, web_event_count
