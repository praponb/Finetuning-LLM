SELECT a.id, a.account_type, COUNT(t.id) AS transaction_count
FROM accounts a
LEFT JOIN transactions t ON t.account_id = a.id
WHERE t.amount >= 2400
GROUP BY a.id, a.account_type;
