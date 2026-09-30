WITH account_stats AS (
    SELECT account_id, AVG(amount) AS avg_amount, COUNT(DISTINCT amount) AS unique_txn_types
    FROM transactions
    WHERE txn_type = 'DEBIT'
    GROUP BY account_id
)
SELECT a.opened_date, as.avg_amount, as.unique_txn_types
FROM accounts a
INNER JOIN account_stats as ON a.id = as.account_id;
