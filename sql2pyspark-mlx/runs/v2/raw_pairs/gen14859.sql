SELECT
    we.id,
    we.event_type,
    we.duration_sec,
    s.customer_id
FROM web_events we
LEFT JOIN sessions s ON we.session_id = s.id;
