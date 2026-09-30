SELECT a.status, MIN(t.amount) AS min_amount
FROM transactions t
INNER JOIN accounts a ON a.id = t.account_id
INNER JOIN customers c ON c.id = a.customer_id
WHERE t.is_flagged
GROUP BY a.status
ORDER BY min_amount DESC, status
LIMIT 10
