SELECT CASE WHEN a.balance >= 58000 THEN 'high' WHEN a.balance >= 15000 THEN 'mid' ELSE 'low' END AS balance_bucket, SUM(t.amount) AS total_amount, MAX(t.amount) AS max_amount
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
WHERE t.txn_time < '2024-11-15'
GROUP BY CASE WHEN a.balance >= 58000 THEN 'high' WHEN a.balance >= 15000 THEN 'mid' ELSE 'low' END
ORDER BY balance_bucket;
