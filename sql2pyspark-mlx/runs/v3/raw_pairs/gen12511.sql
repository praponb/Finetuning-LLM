SELECT
    we.event_type,
    we.event_time,
    s.started_at,
    s.country,
    c.segment,
    c.is_active
FROM web_events we
JOIN sessions s ON we.session_id = s.id
LEFT JOIN customers c ON s.customer_id = c.id
WHERE we.event_time >= '2024-08-18' AND s.device = 'tablet'
