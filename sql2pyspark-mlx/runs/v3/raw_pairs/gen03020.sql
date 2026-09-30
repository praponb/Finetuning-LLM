SELECT
    we.id,
    we.page,
    we.session_id,
    we.event_time,
    s.country,
    s.started_at,
    s.customer_id
FROM web_events we
LEFT JOIN sessions s ON we.session_id = s.id;
