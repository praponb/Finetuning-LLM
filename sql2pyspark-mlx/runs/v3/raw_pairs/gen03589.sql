SELECT t.id, t.amount, a.account_type FROM transactions t JOIN accounts a ON t.account_id = a.id
