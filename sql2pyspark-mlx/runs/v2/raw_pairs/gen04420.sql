SELECT
    we.event_type,
    we.event_time,
    s.started_at,
    c.email
FROM web_events we
RIGHT JOIN sessions s ON we.session_id = s.id
INNER JOIN customers c ON c.id = s.customer_id
WHERE we.page IN ('/cart', '/checkout');
