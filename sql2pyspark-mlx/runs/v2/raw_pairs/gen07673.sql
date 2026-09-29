SELECT a.status, COUNT(*) AS transaction_count
FROM transactions t
LEFT JOIN accounts a ON t.account_id = a.id
WHERE t.account_id IN (20, 25, 26)
GROUP BY a.status
ORDER BY a.status;
