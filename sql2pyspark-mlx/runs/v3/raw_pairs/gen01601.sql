SELECT
    s.browser,
    s.country,
    s.started_at,
    c.name,
    c.country AS customer_country
FROM sessions s
INNER JOIN customers c ON c.id = s.customer_id
WHERE s.customer_id IN (7, 14, 21, 28)
