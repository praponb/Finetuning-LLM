SELECT id, account_id FROM transactions WHERE amount > 2400 UNION ALL SELECT id, account_id FROM transactions WHERE account_id IN (8, 10, 15);
