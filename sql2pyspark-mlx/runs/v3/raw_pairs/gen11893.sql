WITH account_totals AS (
    SELECT account_id, MIN(txn_time) AS first_txn_time
    FROM transactions
    GROUP BY account_id
)
SELECT accounts.id, accounts.balance, account_totals.first_txn_time
FROM accounts
JOIN account_totals ON accounts.id = account_totals.account_id
ORDER BY accounts.id DESC
