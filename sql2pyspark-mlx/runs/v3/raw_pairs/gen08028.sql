SELECT
    we.id,
    we.session_id,
    s.started_at,
    c.name
FROM web_events we
LEFT JOIN sessions s ON we.session_id = s.id
INNER JOIN customers c ON s.customer_id = c.id
WHERE we.event_time >= '2025-04-01';
