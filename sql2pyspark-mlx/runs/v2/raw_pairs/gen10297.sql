SELECT CASE WHEN balance >= 78000 THEN 'high' WHEN balance >= 20000 THEN 'mid' ELSE 'low' END AS balance_bucket, MAX(opened_date) AS last_opened_date, COUNT(balance) AS balance_count
FROM accounts
GROUP BY CASE WHEN balance >= 78000 THEN 'high' WHEN balance >= 20000 THEN 'mid' ELSE 'low' END
HAVING balance_count >= 2
ORDER BY balance_count, balance_bucket
LIMIT 10
