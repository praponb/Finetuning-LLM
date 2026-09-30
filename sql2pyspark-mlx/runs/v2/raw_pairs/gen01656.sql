SELECT
    sessions.started_at,
    sessions.browser,
    sessions.customer_id,
    customers.email,
    customers.segment,
    customers.age,
    CEIL(customers.age) age_whole
FROM sessions
FULL OUTER JOIN customers ON sessions.customer_id = customers.id
WHERE sessions.customer_id IN (3, 4, 15, 17) AND customers.credit_limit BETWEEN 5600 AND 15200;
