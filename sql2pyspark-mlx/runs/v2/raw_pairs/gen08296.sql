SELECT
    we.id,
    we.session_id,
    s.customer_id,
    s.started_at,
    c.is_active
FROM web_events we
JOIN sessions s ON we.session_id = s.id
JOIN customers c ON s.customer_id = c.id
WHERE we.session_id IN (4, 9, 20, 37)
ORDER BY we.session_id DESC
