SELECT a.status, a.customer_id, COUNT(*) AS transaction_count FROM transactions t LEFT JOIN accounts a ON t.account_id = a.id GROUP BY a.status, a.customer_id ORDER BY a.status
