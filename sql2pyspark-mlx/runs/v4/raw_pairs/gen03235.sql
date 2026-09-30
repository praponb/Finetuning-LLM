WITH account_summary AS (
    SELECT account_id, ROUND(STDDEV(amount), 2) AS stddev_amount
    FROM transactions
    WHERE merchant IN ('Shell', 'Starbucks')
    GROUP BY account_id
)
SELECT a.balance, as.stddev_amount
FROM accounts a
INNER JOIN account_summary as ON a.id = as.account_id
WHERE as.stddev_amount >= 38
ORDER BY balance DESC
