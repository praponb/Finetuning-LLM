SELECT
    we.session_id,
    we.page,
    we.event_time,
    s.country,
    s.device
FROM web_events we
LEFT JOIN sessions s ON we.session_id = s.id
