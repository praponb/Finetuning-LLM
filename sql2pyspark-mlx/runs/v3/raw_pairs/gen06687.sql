SELECT
    we.id,
    we.page,
    s.started_at,
    s.browser,
    c.name,
    c.email
FROM web_events we
RIGHT OUTER JOIN sessions s ON we.session_id = s.id
INNER JOIN customers c ON c.id = s.customer_id
WHERE MONTH(c.signup_date) IN (8, 11, 12)
