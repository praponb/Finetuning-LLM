SELECT a.status, COUNT(t.amount) AS amount_count, MAX(t.txn_time) AS latest_txn_time
FROM transactions t
LEFT JOIN accounts a ON t.account_id = a.id
WHERE t.merchant NOT IN ('Amazon', 'Shell')
GROUP BY a.status
ORDER BY status
LIMIT 1
