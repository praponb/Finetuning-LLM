SELECT DATE_TRUNC('MONTH', opened_date) AS opened_month, CASE WHEN balance >= 60000 THEN 'high' WHEN balance >= 34000 THEN 'mid' ELSE 'low' END AS balance_bucket, COUNT(CASE WHEN account_type = 'SAVINGS' THEN 1 END) AS savings_count
FROM accounts
WHERE balance > 42000 AND (status IN ('FROZEN', 'OPEN') OR account_type = 'SAVINGS')
GROUP BY DATE_TRUNC('MONTH', opened_date), CASE WHEN balance >= 60000 THEN 'high' WHEN balance >= 34000 THEN 'mid' ELSE 'low' END
HAVING COUNT(CASE WHEN account_type = 'SAVINGS' THEN 1 END) > 4
ORDER BY opened_month, 2
LIMIT 1
