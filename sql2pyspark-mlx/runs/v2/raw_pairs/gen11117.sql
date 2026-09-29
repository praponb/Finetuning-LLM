SELECT a.id, COUNT(t.id) AS transaction_count, SUM(t.amount) AS total_amount
FROM accounts a
LEFT JOIN transactions t ON a.id = t.account_id
GROUP BY a.id
ORDER BY id, total_amount DESC
LIMIT 3;
