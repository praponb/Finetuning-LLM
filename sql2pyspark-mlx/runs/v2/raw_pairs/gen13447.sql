WITH account_stats AS (
    SELECT account_id, COUNT(DISTINCT amount) AS unique_txn_types
    FROM transactions
    WHERE amount BETWEEN 3000 AND 3700
    GROUP BY account_id
)
SELECT a.opened_date, a.customer_id, as.unique_txn_types
FROM accounts a
INNER JOIN account_stats as ON a.id = as.account_id
WHERE as.unique_txn_types >= 5
