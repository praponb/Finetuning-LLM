SELECT id, amount, merchant, account_id FROM transactions WHERE amount > (SELECT MAX(amount) FROM transactions) ORDER BY account_id DESC;
