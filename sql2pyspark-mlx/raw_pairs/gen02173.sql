SELECT
    we.id,
    we.session_id,
    we.event_time,
    s.device,
    s.customer_id
FROM web_events we
LEFT JOIN sessions s ON we.session_id = s.id;
