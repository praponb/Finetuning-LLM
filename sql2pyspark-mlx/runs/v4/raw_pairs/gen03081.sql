SELECT CASE WHEN a.balance >= 59000 THEN 'high' WHEN a.balance >= 31000 THEN 'mid' ELSE 'low' END AS balance_bucket, MIN(t.amount) AS min_amount, SUM(CASE WHEN t.merchant = 'Starbucks' THEN 1 ELSE 0 END) AS starbucks_count
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
WHERE t.txn_time < '2025-01-01'
GROUP BY CASE WHEN a.balance >= 59000 THEN 'high' WHEN a.balance >= 31000 THEN 'mid' ELSE 'low' END;
