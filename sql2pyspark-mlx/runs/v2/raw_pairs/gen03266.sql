SELECT a.id, COUNT(t.id) AS transaction_count, SUM(t.amount) AS total_amount
FROM accounts a
INNER JOIN transactions t ON t.account_id = a.id
WHERE a.status = 'OPEN'
GROUP BY a.id
ORDER BY id DESC;
