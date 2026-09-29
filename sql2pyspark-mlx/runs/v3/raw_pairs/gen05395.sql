SELECT
    s.id,
    s.end_date,
    s.customer_id,
    s.start_date,
    c.email,
    c.country
FROM subscriptions s
FULL OUTER JOIN customers c ON s.customer_id = c.id
ORDER BY country DESC
