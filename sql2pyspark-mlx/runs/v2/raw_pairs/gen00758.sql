SELECT id, amount, merchant, txn_type FROM transactions WHERE amount < (SELECT MIN(amount) FROM transactions WHERE account_id IN (3, 8, 30, 40)) ORDER BY amount, id DESC LIMIT 50;
