SELECT a.status, COUNT(t.id) AS transaction_count
FROM accounts a
JOIN transactions t ON t.account_id = a.id
WHERE t.account_id IN (4, 23)
GROUP BY a.status
