WITH account_stats AS (
    SELECT account_id, AVG(amount) avg_amount, SUM(amount) total_amount
    FROM transactions
    GROUP BY account_id
)
SELECT a.customer_id, as.avg_amount, as.total_amount
FROM accounts a
INNER JOIN account_stats as ON a.id = as.account_id
WHERE as.avg_amount >= 3000
