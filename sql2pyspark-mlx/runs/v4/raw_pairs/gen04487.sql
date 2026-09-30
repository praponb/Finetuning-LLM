SELECT accounts.balance, transactions.txn_time
FROM accounts
INNER JOIN transactions ON accounts.id = transactions.account_id
WHERE YEAR(accounts.opened_date) = 2025;
