SELECT
    s.id,
    s.customer_id,
    c.country,
    c.is_active,
    TO_DATE(s.started_at) AS started_at_date
FROM sessions s
LEFT JOIN customers c ON s.customer_id = c.id
ORDER BY country, id
LIMIT 20
