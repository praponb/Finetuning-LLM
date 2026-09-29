SELECT
    we.id,
    we.session_id,
    we.event_type,
    s.country,
    c.is_active
FROM web_events we
LEFT OUTER JOIN sessions s ON we.session_id = s.id
INNER JOIN customers c ON c.id = s.customer_id;
