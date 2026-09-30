SELECT s.started_at, s.device, we.duration_sec, we.event_time FROM sessions s LEFT OUTER JOIN web_events we ON s.id = we.session_id WHERE s.started_at < '2025-03-01';
