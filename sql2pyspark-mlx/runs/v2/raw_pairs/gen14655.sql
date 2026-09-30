SELECT CASE WHEN balance >= 80000 THEN 'high' WHEN balance >= 34000 THEN 'mid' ELSE 'low' END balance_bucket, account_type, AVG(balance) avg_balance
FROM accounts
WHERE balance <= 73000 OR status IS NULL
GROUP BY CASE WHEN balance >= 80000 THEN 'high' WHEN balance >= 34000 THEN 'mid' ELSE 'low' END, account_type
