SELECT
    we.event_time,
    we.session_id,
    s.browser,
    s.country,
    c.country AS customer_country
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
INNER JOIN customers c ON s.customer_id = c.id
ORDER BY 4 ASC
