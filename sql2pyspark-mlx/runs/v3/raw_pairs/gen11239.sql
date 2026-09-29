SELECT
    status,
    CASE WHEN balance >= 72000 THEN 'high' WHEN balance >= 19000 THEN 'mid' ELSE 'low' END AS balance_bucket,
    ROUND(AVG(balance), 2) AS avg_balance,
    COUNT(*) AS cnt
FROM accounts
GROUP BY 1, 2
ORDER BY status, balance_bucket
