SELECT
    we.id,
    we.event_time,
    we.event_type,
    s.customer_id,
    c.signup_date,
    c.name
FROM web_events we
RIGHT JOIN sessions s ON we.session_id = s.id
INNER JOIN customers c ON s.customer_id = c.id
WHERE s.customer_id IN (17, 18, 27, 28) OR s.country = 'UK'
ORDER BY signup_date ASC;
