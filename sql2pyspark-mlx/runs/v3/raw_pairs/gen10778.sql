SELECT c.is_active, MAX(t.txn_time) AS latest_txn_time, ROUND((100 * SUM(CASE WHEN t.merchant = 'Starbucks' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_starbucks
FROM transactions t
FULL OUTER JOIN accounts a ON t.account_id = a.id
JOIN customers c ON c.id = a.customer_id
GROUP BY c.is_active
HAVING ROUND((100 * SUM(CASE WHEN t.merchant = 'Starbucks' THEN 1 ELSE 0 END)) / COUNT(*), 1) > 62
