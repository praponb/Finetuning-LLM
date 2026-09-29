SELECT transactions.id, transactions.amount FROM transactions WHERE transactions.account_id = 32 AND transactions.account_id NOT IN (SELECT id FROM accounts WHERE customer_id IS NULL);
