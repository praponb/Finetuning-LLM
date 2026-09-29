SELECT CASE WHEN a.balance >= 60000 THEN 'high' WHEN a.balance >= 34000 THEN 'mid' ELSE 'low' END AS balance_bucket, DATE_TRUNC('MONTH', a.opened_date) AS opened_month, MAX(t.txn_time) AS latest_txn_time
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
GROUP BY CASE WHEN a.balance >= 60000 THEN 'high' WHEN a.balance >= 34000 THEN 'mid' ELSE 'low' END, DATE_TRUNC('MONTH', a.opened_date)
ORDER BY latest_txn_time, opened_month DESC;
