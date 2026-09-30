SELECT
    we.id,
    we.page,
    we.event_time,
    s.customer_id,
    s.browser,
    c.email
FROM web_events we
LEFT JOIN sessions s ON we.session_id = s.id
LEFT JOIN customers c ON s.customer_id = c.id
WHERE we.event_type = 'add_to_cart' AND c.name LIKE '%Kim%'
