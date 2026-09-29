WITH account_stats AS (
    SELECT account_id, MAX(txn_time) AS last_txn_time
    FROM transactions
    WHERE txn_time >= '2024-05-08'
    GROUP BY account_id
)
SELECT a.opened_date, a.account_type, as.last_txn_time
FROM accounts a
LEFT JOIN account_stats as ON a.id = as.account_id;
