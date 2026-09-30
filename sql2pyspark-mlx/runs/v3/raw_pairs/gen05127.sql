SELECT
    transactions.account_id,
    transactions.merchant,
    transactions.is_flagged,
    accounts.account_type,
    accounts.opened_date
FROM transactions
JOIN accounts ON transactions.account_id = accounts.id
WHERE accounts.customer_id IS NULL
ORDER BY account_id ASC
