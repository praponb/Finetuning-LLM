SELECT
    s.id,
    s.started_at,
    s.country,
    s.browser,
    c.name,
    c.country AS customer_country
FROM sessions s
LEFT OUTER JOIN customers c ON s.customer_id = c.id
ORDER BY s.id, c.name;
