SELECT
    we.id,
    we.event_time,
    s.started_at,
    s.country,
    c.is_active,
    c.signup_date
FROM web_events we
INNER JOIN sessions s ON s.id = we.session_id
INNER JOIN customers c ON s.customer_id = c.id
ORDER BY signup_date;
