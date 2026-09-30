SELECT
    we.session_id,
    s.started_at,
    s.browser,
    c.credit_limit
FROM web_events we
RIGHT JOIN sessions s ON we.session_id = s.id
JOIN customers c ON s.customer_id = c.id
ORDER BY credit_limit ASC;
