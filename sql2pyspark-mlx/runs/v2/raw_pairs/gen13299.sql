SELECT
    we.session_id,
    we.duration_sec,
    s.started_at,
    s.device,
    c.credit_limit,
    c.country
FROM web_events we
INNER JOIN sessions s ON s.id = we.session_id
INNER JOIN customers c ON s.customer_id = c.id
ORDER BY s.started_at
