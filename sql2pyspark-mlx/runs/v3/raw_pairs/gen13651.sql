SELECT a.status, COUNT(DISTINCT t.amount) AS unique_txn_types, MIN(t.amount) AS min_amount
FROM transactions t
LEFT JOIN accounts a ON t.account_id = a.id
GROUP BY a.status
ORDER BY unique_txn_types, status
LIMIT 1
