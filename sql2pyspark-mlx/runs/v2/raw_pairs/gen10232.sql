SELECT
    we.id,
    we.event_time,
    s.country,
    s.device,
    c.name,
    c.country customer_country
FROM web_events we
LEFT JOIN sessions s ON we.session_id = s.id
INNER JOIN customers c ON s.customer_id = c.id
WHERE we.session_id IN (14, 17);
