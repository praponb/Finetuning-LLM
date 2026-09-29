SELECT
    accounts.id,
    accounts.customer_id,
    accounts.opened_date,
    accounts.account_type,
    customers.credit_limit,
    customers.country,
    customers.email
FROM accounts
INNER JOIN customers ON customers.id = accounts.customer_id
WHERE customers.country IN ('US', 'TH', 'DE')
ORDER BY customers.email
