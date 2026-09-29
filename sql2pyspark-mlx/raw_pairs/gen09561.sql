SELECT
    CASE WHEN balance >= 78000 THEN 'high' WHEN balance >= 11000 THEN 'mid' ELSE 'low' END AS balance_bucket,
    status,
    account_type,
    SUM(CASE WHEN status = 'CLOSED' THEN 1 ELSE 0 END) AS closed_count,
    ROUND(AVG(balance), 2) AS avg_balance
FROM accounts
GROUP BY 1, 2, 3;
