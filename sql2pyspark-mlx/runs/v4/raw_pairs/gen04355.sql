SELECT CASE WHEN balance >= 87000 THEN 'high' WHEN balance >= 33000 THEN 'mid' ELSE 'low' END AS balance_bucket, MAX(balance) AS max_balance, MIN(balance) AS min_balance
FROM accounts
GROUP BY 1
ORDER BY balance_bucket, min_balance;
