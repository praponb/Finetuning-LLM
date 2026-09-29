SELECT a.status, MAX(t.amount) - MIN(t.amount) AS amount_range
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
WHERE t.txn_time < '2025-08-16'
GROUP BY a.status
