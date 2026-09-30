SELECT
    a.customer_id,
    a.status,
    c.segment,
    c.country
FROM accounts a
RIGHT OUTER JOIN customers c ON c.id = a.customer_id
ORDER BY country ASC
