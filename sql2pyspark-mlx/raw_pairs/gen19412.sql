SELECT
    accounts.status,
    accounts.opened_date,
    transactions.txn_type,
    transactions.account_id
FROM accounts
LEFT JOIN transactions ON accounts.id = transactions.account_id
ORDER BY accounts.status DESC;
