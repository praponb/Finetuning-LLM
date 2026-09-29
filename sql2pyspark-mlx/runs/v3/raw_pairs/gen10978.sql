SELECT
    s.started_at,
    s.country,
    c.country AS customer_country,
    c.age
FROM sessions s
INNER JOIN customers c ON s.customer_id = c.id;
