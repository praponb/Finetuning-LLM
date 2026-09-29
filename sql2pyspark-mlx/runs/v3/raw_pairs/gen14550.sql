SELECT
    accounts.id,
    accounts.customer_id,
    accounts.status,
    customers.credit_limit,
    customers.email
FROM accounts
JOIN customers ON accounts.customer_id = customers.id;
