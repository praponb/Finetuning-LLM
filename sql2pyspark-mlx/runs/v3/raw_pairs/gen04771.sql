WITH account_stats AS (
    SELECT account_id, ROUND(STDDEV(amount), 2) stddev_amount, MIN(amount) min_amount
    FROM transactions
    WHERE account_id IN (4, 30, 36)
    GROUP BY account_id
)
SELECT
    a.id,
    a.status,
    as.stddev_amount,
    as.min_amount
FROM accounts a
INNER JOIN account_stats as ON a.id = as.account_id
ORDER BY a.status, id
LIMIT 3
