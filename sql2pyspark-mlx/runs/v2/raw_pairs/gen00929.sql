SELECT sessions.started_at, sessions.country, customers.credit_limit
FROM sessions
LEFT JOIN customers ON sessions.customer_id = customers.id
WHERE sessions.browser IN ('Edge', 'Chrome')
ORDER BY country DESC;
