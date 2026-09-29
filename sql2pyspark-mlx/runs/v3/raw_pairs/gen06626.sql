SELECT
    we.id,
    we.event_time,
    we.page,
    s.country,
    s.device,
    c.country AS customer_country
FROM web_events we
FULL OUTER JOIN sessions s ON we.session_id = s.id
JOIN customers c ON s.customer_id = c.id;
