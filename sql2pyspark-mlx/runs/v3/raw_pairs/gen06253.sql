SELECT s.device, we.event_type
FROM sessions s
INNER JOIN web_events we ON we.session_id = s.id
WHERE s.country IN ('TH', 'JP')
ORDER BY s.device;
