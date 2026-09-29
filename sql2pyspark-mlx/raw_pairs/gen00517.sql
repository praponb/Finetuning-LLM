SELECT
    we.id,
    we.page,
    s.browser,
    c.credit_limit,
    c.country
FROM web_events we
INNER JOIN sessions s ON s.id = we.session_id
INNER JOIN customers c ON c.id = s.customer_id
WHERE we.duration_sec BETWEEN 223 AND 448 OR c.name LIKE '%Park%';
