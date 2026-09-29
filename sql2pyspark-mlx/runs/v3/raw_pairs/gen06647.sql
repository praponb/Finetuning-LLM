SELECT a.id, a.account_type, COUNT(t.id) AS transaction_count
FROM accounts a
INNER JOIN transactions t ON t.account_id = a.id
WHERE a.customer_id IS NOT NULL
GROUP BY a.id, a.account_type
