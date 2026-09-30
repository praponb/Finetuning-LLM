SELECT
    transactions.id,
    transactions.amount,
    accounts.customer_id,
    accounts.account_type,
    accounts.balance,
    LOWER(accounts.account_type) AS account_type_lower
FROM transactions
LEFT JOIN accounts ON accounts.id = transactions.account_id
WHERE accounts.customer_id IS NULL
ORDER BY account_type_lower, account_type, id
LIMIT 1
