SELECT
    accounts.id,
    accounts.status,
    customers.name,
    customers.credit_limit
FROM accounts
LEFT JOIN customers ON customers.id = accounts.customer_id
ORDER BY name DESC;
