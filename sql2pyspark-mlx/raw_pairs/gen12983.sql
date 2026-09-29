SELECT
    accounts.status,
    accounts.customer_id,
    accounts.account_type,
    customers.signup_date,
    customers.segment
FROM accounts
INNER JOIN customers ON customers.id = accounts.customer_id
WHERE customers.credit_limit IS NULL;
