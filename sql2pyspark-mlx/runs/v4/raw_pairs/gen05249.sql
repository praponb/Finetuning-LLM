SELECT
    sessions.country,
    sessions.browser,
    sessions.started_at,
    customers.credit_limit,
    customers.city
FROM sessions
RIGHT JOIN customers ON customers.id = sessions.customer_id
WHERE sessions.customer_id IN (21, 26, 30, 34)
