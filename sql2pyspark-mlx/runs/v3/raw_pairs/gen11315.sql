WITH transactions_by_account AS (
    SELECT account_id, MAX(txn_time) AS latest_txn_time, COUNT(*) AS row_count
    FROM transactions
    GROUP BY account_id
)
SELECT a.balance, tba.latest_txn_time, tba.row_count
FROM accounts a
LEFT JOIN transactions_by_account tba ON a.id = tba.account_id
ORDER BY tba.latest_txn_time;
