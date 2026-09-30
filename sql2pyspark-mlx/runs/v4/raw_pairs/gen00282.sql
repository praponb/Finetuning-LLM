SELECT
    we.page,
    we.event_time,
    s.started_at,
    c.signup_date
FROM web_events we
JOIN sessions s ON we.session_id = s.id
JOIN customers c ON c.id = s.customer_id
