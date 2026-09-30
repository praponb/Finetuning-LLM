SELECT
    we.duration_sec,
    we.event_time,
    s.started_at,
    c.name
FROM web_events we
LEFT JOIN sessions s ON we.session_id = s.id
LEFT JOIN customers c ON s.customer_id = c.id
ORDER BY started_at, 1 DESC;
