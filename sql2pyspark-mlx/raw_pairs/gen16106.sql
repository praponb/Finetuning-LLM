SELECT
    we.event_type,
    s.started_at,
    c.age,
    CONCAT(we.event_type, '/', we.page) AS event_type_page
FROM web_events we
INNER JOIN sessions s ON s.id = we.session_id
INNER JOIN customers c ON c.id = s.customer_id
ORDER BY event_type_page, s.started_at;
