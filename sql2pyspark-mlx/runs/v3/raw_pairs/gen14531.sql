SELECT
    transactions.id,
    transactions.is_flagged,
    accounts.opened_date,
    customers.signup_date,
    UPPER(transactions.merchant) merchant_upper
FROM transactions
LEFT JOIN accounts ON accounts.id = transactions.account_id
INNER JOIN customers ON customers.id = accounts.customer_id
WHERE accounts.status = 'OPEN'
