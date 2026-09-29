WITH account_summary AS (
    SELECT account_id, COUNT(DISTINCT amount) AS unique_txn_types
    FROM transactions
    GROUP BY account_id
)
SELECT a.status, as.unique_txn_types
FROM accounts a
INNER JOIN account_summary as ON a.id = as.account_id;
