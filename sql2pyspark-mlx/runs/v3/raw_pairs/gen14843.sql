SELECT CASE WHEN balance >= 78000 THEN 'high' WHEN balance >= 34000 THEN 'mid' ELSE 'low' END AS balance_bucket, account_type, COUNT(DISTINCT balance) AS unique_statuss FROM accounts GROUP BY 1, 2;
