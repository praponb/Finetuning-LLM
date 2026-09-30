SELECT a.id, a.account_type, COUNT(t.id) AS transaction_count FROM accounts a LEFT OUTER JOIN transactions t ON a.id = t.account_id GROUP BY a.id, a.account_type;
