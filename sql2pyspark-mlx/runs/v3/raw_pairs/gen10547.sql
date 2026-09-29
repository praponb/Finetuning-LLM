SELECT a.account_type, COUNT(*) AS num_transactions, MAX(t.amount) - MIN(t.amount) AS amount_range
FROM transactions t
LEFT JOIN accounts a ON a.id = t.account_id
GROUP BY a.account_type;
