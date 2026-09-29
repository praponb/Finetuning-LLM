SELECT
    s.browser,
    s.device,
    s.customer_id,
    c.name,
    c.segment,
    c.country
FROM sessions s
INNER JOIN customers c ON s.customer_id = c.id;
