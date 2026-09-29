SELECT a.customer_id, a.account_type, a.balance, t.account_id, t.amount FROM accounts a INNER JOIN transactions t ON t.account_id = a.id WHERE t.account_id IN (3, 10, 34)
