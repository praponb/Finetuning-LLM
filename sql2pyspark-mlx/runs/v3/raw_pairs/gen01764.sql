SELECT a.id, COUNT(t.id) transaction_count FROM accounts a LEFT JOIN transactions t ON a.id = t.account_id WHERE t.account_id IN (9, 12) GROUP BY a.id ORDER BY a.id DESC, transaction_count
