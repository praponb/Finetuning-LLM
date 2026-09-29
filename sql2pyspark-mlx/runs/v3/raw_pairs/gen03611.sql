SELECT
    accounts.id,
    accounts.opened_date,
    customers.signup_date,
    customers.credit_limit,
    customers.is_active
FROM accounts
LEFT JOIN customers ON accounts.customer_id = customers.id
WHERE accounts.customer_id IS NULL
