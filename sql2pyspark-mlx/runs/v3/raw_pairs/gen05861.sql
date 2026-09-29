SELECT
    subscriptions.id,
    subscriptions.start_date,
    customers.signup_date,
    customers.city,
    customers.country
FROM subscriptions
LEFT OUTER JOIN customers ON customers.id = subscriptions.customer_id
WHERE UPPER(TRIM(customers.email)) = 'CAROL@COMPANY.CO'
ORDER BY subscriptions.start_date DESC
