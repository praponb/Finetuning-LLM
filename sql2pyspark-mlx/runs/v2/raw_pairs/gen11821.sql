SELECT
    we.event_type,
    s.device,
    c.email,
    c.is_active,
    DATE_TRUNC('YEAR', s.started_at) AS started_at_period
FROM web_events we
LEFT JOIN sessions s ON s.id = we.session_id
LEFT JOIN customers c ON s.customer_id = c.id
WHERE we.event_type = 'add_to_cart'
ORDER BY s.device;
