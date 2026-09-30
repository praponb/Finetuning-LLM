SELECT
    s.id,
    s.country,
    c.country AS customer_country,
    c.segment
FROM sessions s
JOIN customers c ON s.customer_id = c.id;
