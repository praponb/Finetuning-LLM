SELECT
    s.device,
    we.session_id,
    we.event_type,
    we.duration_sec
FROM sessions s
INNER JOIN web_events we ON we.session_id = s.id
