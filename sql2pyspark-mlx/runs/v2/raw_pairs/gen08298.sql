SELECT
    s.browser,
    s.started_at,
    c.email,
    c.segment,
    c.age
FROM sessions s
INNER JOIN customers c ON c.id = s.customer_id;
