SELECT a.id, COUNT(t.id) AS transaction_count, ROUND(AVG(t.amount), 2) AS avg_amount
FROM accounts a
INNER JOIN transactions t ON t.account_id = a.id
WHERE t.account_id IN (5, 21, 29, 38)
GROUP BY a.id
ORDER BY a.id
LIMIT 50;
