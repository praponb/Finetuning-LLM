SELECT a.id, COUNT(t.id) AS transaction_count, ROUND(AVG(t.amount), 2) AS avg_amount
FROM accounts a
LEFT JOIN transactions t ON t.account_id = a.id
WHERE a.opened_date > '2023-07-01'
GROUP BY a.id
ORDER BY 2 DESC, id
LIMIT 10
