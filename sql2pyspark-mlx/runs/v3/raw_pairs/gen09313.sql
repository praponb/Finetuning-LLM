SELECT s.device, MIN(we.event_time) min_event_time
FROM web_events we
JOIN sessions s ON we.session_id = s.id
GROUP BY s.device
ORDER BY min_event_time DESC, device DESC
LIMIT 1
