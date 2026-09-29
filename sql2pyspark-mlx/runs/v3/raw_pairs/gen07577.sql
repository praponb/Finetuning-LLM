WITH account_stats AS (
    SELECT account_id, COUNT(CASE WHEN txn_type = 'CREDIT' THEN 1 END) AS credit_count
    FROM transactions
    GROUP BY account_id
)
SELECT a.account_type, a.opened_date, as.credit_count
FROM accounts a
INNER JOIN account_stats as ON a.id = as.account_id
ORDER BY as.credit_count
