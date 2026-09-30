SELECT s.id, s.device, COUNT(we.id) web_event_count
FROM sessions s
LEFT JOIN web_events we ON s.id = we.session_id
GROUP BY s.id, s.device
HAVING COUNT(we.id) > 5
ORDER BY s.id, device
LIMIT 50
