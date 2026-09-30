SELECT
    we.id,
    we.duration_sec,
    s.started_at,
    s.customer_id,
    c.credit_limit
FROM web_events we
LEFT JOIN sessions s ON we.session_id = s.id
INNER JOIN customers c ON s.customer_id = c.id
ORDER BY credit_limit DESC;
