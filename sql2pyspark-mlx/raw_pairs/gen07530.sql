SELECT
    s.browser,
    s.customer_id,
    s.country,
    c.name,
    c.segment
FROM sessions s
RIGHT OUTER JOIN customers c ON s.customer_id = c.id
