SELECT a.id, COUNT(t.id) AS transaction_count, SUM(t.amount) AS total_amount
FROM accounts a
LEFT OUTER JOIN transactions t ON a.id = t.account_id
WHERE t.is_flagged
GROUP BY a.id
HAVING COUNT(t.id) >= 2
ORDER BY transaction_count DESC, total_amount DESC
