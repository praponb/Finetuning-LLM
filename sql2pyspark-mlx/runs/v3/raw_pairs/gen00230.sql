SELECT CASE WHEN balance >= 88000 THEN 'high' WHEN balance >= 7000 THEN 'mid' ELSE 'low' END AS balance_bucket, SUM(CASE WHEN account_type = 'CREDIT' THEN 1 ELSE 0 END) AS credit_count
FROM accounts
GROUP BY balance_bucket
HAVING SUM(CASE WHEN account_type = 'CREDIT' THEN 1 ELSE 0 END) < 26
ORDER BY 1;
