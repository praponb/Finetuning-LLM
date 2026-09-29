WITH account_stats AS (
    SELECT account_id, MIN(amount) AS min_amount, MIN(txn_time) AS min_txn_time
    FROM transactions
    GROUP BY account_id
)
SELECT
    a.id,
    a.balance,
    as.min_amount,
    as.min_txn_time
FROM accounts a
INNER JOIN account_stats as ON a.id = as.account_id;
