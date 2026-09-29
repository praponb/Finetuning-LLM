SELECT id, amount, merchant, account_id FROM transactions WHERE amount > (SELECT AVG(amount) FROM transactions)
