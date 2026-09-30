SELECT
    sessions.id,
    sessions.browser,
    sessions.country,
    customers.age,
    customers.segment
FROM sessions
FULL OUTER JOIN customers ON sessions.customer_id = customers.id
WHERE YEAR(customers.signup_date) = 2023
ORDER BY customers.segment ASC;
