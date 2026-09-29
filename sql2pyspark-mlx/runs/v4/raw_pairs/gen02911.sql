SELECT
    we.id,
    we.duration_sec,
    we.event_time,
    we.event_type,
    s.started_at,
    s.country
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
WHERE we.session_id = 16
