SELECT
    sessions.id,
    sessions.country,
    sessions.started_at,
    customers.segment,
    customers.name,
    customers.age
FROM sessions
INNER JOIN customers ON sessions.customer_id = customers.id;
