WITH account_stats AS (
    SELECT account_id, AVG(amount) AS avg_amount, COUNT(*) AS row_count
    FROM transactions
    GROUP BY account_id
)
SELECT
    a.id,
    a.status,
    a.balance,
    as.avg_amount,
    as.row_count
FROM accounts a
INNER JOIN account_stats as ON a.id = as.account_id
WHERE as.row_count > 5
