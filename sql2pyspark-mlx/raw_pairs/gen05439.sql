SELECT
    we.id,
    we.duration_sec,
    s.started_at,
    s.customer_id,
    c.name
FROM web_events we
LEFT OUTER JOIN sessions s ON s.id = we.session_id
LEFT OUTER JOIN customers c ON s.customer_id = c.id
WHERE we.event_time >= '2025-10-18' AND we.session_id IN (6, 27, 32);
