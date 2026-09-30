SELECT
    we.id,
    we.event_time,
    s.browser,
    s.started_at,
    c.city
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
INNER JOIN customers c ON c.id = s.customer_id
WHERE HOUR(we.event_time) BETWEEN 9 AND 16;
