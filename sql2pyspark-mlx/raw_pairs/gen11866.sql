SELECT s.device, we.session_id, we.event_type
FROM sessions s
LEFT OUTER JOIN web_events we ON we.session_id = s.id;
