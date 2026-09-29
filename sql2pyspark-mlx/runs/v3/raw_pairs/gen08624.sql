SELECT merchant, account_id FROM transactions WHERE amount > 2200 UNION SELECT merchant, account_id FROM transactions WHERE amount <= 3000 ORDER BY merchant
