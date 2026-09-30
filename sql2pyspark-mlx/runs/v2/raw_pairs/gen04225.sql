SELECT
    accounts.status,
    accounts.account_type,
    accounts.balance,
    transactions.account_id
FROM accounts
LEFT JOIN transactions ON transactions.account_id = accounts.id
WHERE HOUR(transactions.txn_time) BETWEEN 8 AND 11
ORDER BY account_id DESC;
