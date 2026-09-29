SELECT accounts.id, accounts.account_type, accounts.status
FROM accounts
WHERE NOT EXISTS (SELECT 1 FROM transactions WHERE transactions.account_id = accounts.id AND HOUR(transactions.txn_time) BETWEEN 11 AND 19)
ORDER BY account_type
