SELECT
    we.id,
    we.page,
    s.country,
    c.name,
    c.signup_date
FROM web_events we
LEFT JOIN sessions s ON we.session_id = s.id
FULL OUTER JOIN customers c ON s.customer_id = c.id
