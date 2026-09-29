SELECT CASE WHEN balance >= 58000 THEN 'high' WHEN balance >= 37000 THEN 'mid' ELSE 'low' END AS balance_bucket, ROUND((100 * SUM(CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_open
FROM accounts
GROUP BY 1
HAVING ROUND((100 * SUM(CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END)) / COUNT(*), 1) >= 65;
