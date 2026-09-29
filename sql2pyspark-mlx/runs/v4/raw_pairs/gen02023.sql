SELECT a.id, COUNT(t.id) AS transaction_count, MAX(t.amount) AS max_amount
FROM accounts a
LEFT JOIN transactions t ON a.id = t.account_id
WHERE t.amount < 3500
GROUP BY a.id
ORDER BY 3, id
LIMIT 10
