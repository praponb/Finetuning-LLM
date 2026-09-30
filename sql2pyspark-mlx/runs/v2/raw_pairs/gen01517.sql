SELECT
    we.id,
    we.event_time,
    we.duration_sec,
    s.started_at,
    s.browser,
    s.customer_id
FROM web_events we
JOIN sessions s ON we.session_id = s.id
ORDER BY id DESC, s.customer_id DESC
