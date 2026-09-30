SELECT
    s.browser,
    s.customer_id,
    c.age,
    c.credit_limit
FROM sessions s
FULL OUTER JOIN customers c ON s.customer_id = c.id
WHERE s.customer_id = 14;
