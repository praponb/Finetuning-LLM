SELECT a.id, a.account_type, COUNT(t.id) transaction_count FROM accounts a LEFT OUTER JOIN transactions t ON t.account_id = a.id GROUP BY a.id, a.account_type
