SELECT
    sessions.id,
    sessions.customer_id,
    sessions.browser,
    customers.signup_date
FROM sessions
JOIN customers ON customers.id = sessions.customer_id
WHERE NOT (customers.is_active = FALSE OR sessions.customer_id IN (33, 34))
ORDER BY customers.signup_date
