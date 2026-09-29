SELECT
    CASE WHEN balance >= 85000 THEN 'high' WHEN balance >= 11000 THEN 'mid' ELSE 'low' END AS balance_bucket,
    status,
    ROUND(AVG(balance), 2) AS avg_balance,
    ROUND(STDDEV(balance), 2) AS stddev_balance,
    MIN(balance) AS min_balance
FROM accounts
GROUP BY 1, 2
HAVING avg_balance < 33
ORDER BY stddev_balance DESC;
