SELECT we.duration_sec, s.device, s.started_at
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
WHERE we.duration_sec = 199
