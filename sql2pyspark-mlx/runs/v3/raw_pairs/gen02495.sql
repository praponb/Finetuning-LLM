WITH account_stats AS (
    SELECT account_id, AVG(amount) AS avg_amount
    FROM transactions
    GROUP BY account_id
)
SELECT
    a.id,
    a.status,
    a.customer_id,
    as.avg_amount
FROM accounts a
INNER JOIN account_stats as ON a.id = as.account_id
WHERE as.avg_amount > 1000
ORDER BY avg_amount DESC, id
LIMIT 20
