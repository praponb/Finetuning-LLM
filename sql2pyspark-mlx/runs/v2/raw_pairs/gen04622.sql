SELECT a.status, COUNT(t.amount) AS amount_count, MAX(t.amount) AS max_amount
FROM transactions t
FULL OUTER JOIN accounts a ON t.account_id = a.id
GROUP BY a.status
HAVING COUNT(t.amount) > 3
ORDER BY max_amount
