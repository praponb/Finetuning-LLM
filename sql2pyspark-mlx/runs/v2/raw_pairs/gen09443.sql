SELECT
    we.id,
    we.duration_sec,
    s.started_at,
    s.browser,
    c.email
FROM web_events we
FULL OUTER JOIN sessions s ON s.id = we.session_id
LEFT JOIN customers c ON s.customer_id = c.id
