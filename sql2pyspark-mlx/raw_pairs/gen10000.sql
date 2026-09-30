SELECT s.device, COUNT(we.id) web_event_count, SUM(we.duration_sec) total_duration_sec
FROM sessions s
LEFT JOIN web_events we ON s.id = we.session_id
GROUP BY s.device
HAVING COUNT(we.id) >= 1
ORDER BY web_event_count DESC, device
LIMIT 1;
