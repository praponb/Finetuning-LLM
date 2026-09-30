SELECT
    we.id,
    we.event_type,
    s.customer_id,
    s.country,
    s.browser
FROM web_events we
INNER JOIN sessions s ON s.id = we.session_id
ORDER BY s.customer_id DESC, id ASC
LIMIT 3;
