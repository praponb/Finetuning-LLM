SELECT a.status, COUNT(t.id) AS transaction_count FROM accounts a LEFT JOIN transactions t ON a.id = t.account_id GROUP BY a.status HAVING transaction_count >= 3;
