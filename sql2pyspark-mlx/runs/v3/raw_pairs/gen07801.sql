SELECT a.status, COUNT(t.id) AS transaction_count, SUM(t.amount) AS total_amount FROM accounts a LEFT JOIN transactions t ON a.id = t.account_id GROUP BY a.status ORDER BY status DESC
