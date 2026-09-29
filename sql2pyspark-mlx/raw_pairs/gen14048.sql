SELECT sessions.id, sessions.country, customers.email
FROM sessions
FULL OUTER JOIN customers ON sessions.customer_id = customers.id
WHERE sessions.customer_id IN (12, 37) AND sessions.browser = 'Firefox'
