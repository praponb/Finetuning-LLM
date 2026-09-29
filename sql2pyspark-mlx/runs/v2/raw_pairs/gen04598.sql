SELECT
    accounts.id,
    accounts.balance,
    accounts.account_type,
    accounts.status,
    customers.is_active,
    customers.name,
    customers.signup_date
FROM accounts
JOIN customers ON accounts.customer_id = customers.id
ORDER BY accounts.id DESC, accounts.account_type DESC;
