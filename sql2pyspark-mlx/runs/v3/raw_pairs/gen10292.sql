SELECT
    we.event_type,
    we.duration_sec,
    s.browser,
    s.started_at,
    c.age
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
INNER JOIN customers c ON s.customer_id = c.id;
