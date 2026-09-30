WITH account_stats AS (
    SELECT account_id, ROUND(STDDEV(amount), 2) AS stddev_amount
    FROM transactions
    GROUP BY account_id
)
SELECT a.id, a.opened_date, as.stddev_amount
FROM accounts a
LEFT JOIN account_stats as ON a.id = as.account_id
ORDER BY stddev_amount DESC;
