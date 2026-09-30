SELECT
    we.session_id,
    s.device,
    c.signup_date,
    c.country
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
INNER JOIN customers c ON s.customer_id = c.id
ORDER BY signup_date DESC;
