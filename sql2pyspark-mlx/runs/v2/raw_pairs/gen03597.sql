WITH account_stats AS (
    SELECT account_id, ROUND(STDDEV(amount), 2) AS stddev_amount, ROUND((100 * SUM(CASE WHEN merchant = '7-Eleven' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_7_eleven
    FROM transactions
    WHERE merchant NOT IN ('Shell', 'Grab')
    GROUP BY account_id
)
SELECT
    a.id,
    a.customer_id,
    as.stddev_amount,
    as.pct_7_eleven
FROM accounts a
INNER JOIN account_stats as ON a.id = as.account_id
ORDER BY stddev_amount DESC
