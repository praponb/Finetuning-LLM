SELECT CASE WHEN status IN ('CLOSED', 'FROZEN') THEN 'tier_1' ELSE 'other' END AS status_group, CASE WHEN balance >= 82000 THEN 'high' WHEN balance >= 11000 THEN 'mid' ELSE 'low' END AS balance_bucket, ROUND(AVG(balance), 2) AS avg_balance
FROM accounts
GROUP BY CASE WHEN status IN ('CLOSED', 'FROZEN') THEN 'tier_1' ELSE 'other' END, CASE WHEN balance >= 82000 THEN 'high' WHEN balance >= 11000 THEN 'mid' ELSE 'low' END
ORDER BY balance_bucket
