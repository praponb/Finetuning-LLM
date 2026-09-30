SELECT
    we.id,
    we.session_id,
    we.event_time,
    we.duration_sec,
    s.started_at
FROM web_events we
LEFT JOIN sessions s ON we.session_id = s.id
ORDER BY s.started_at ASC;
