WITH TransactionsByAccount AS (
    SELECT account_id, MAX(txn_time) AS last_txn_time
    FROM transactions
    GROUP BY account_id
)
SELECT a.id, a.balance, T.last_txn_time
FROM accounts a
INNER JOIN TransactionsByAccount T ON a.id = T.account_id
ORDER BY id DESC
