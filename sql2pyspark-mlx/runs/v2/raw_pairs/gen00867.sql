SELECT
    we.id,
    we.event_time,
    s.country,
    c.country AS customer_country,
    c.is_active
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
LEFT OUTER JOIN customers c ON s.customer_id = c.id
ORDER BY 2 DESC, customer_country DESC
