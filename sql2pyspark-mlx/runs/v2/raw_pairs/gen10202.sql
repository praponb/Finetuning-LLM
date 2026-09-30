SELECT transactions.id, transactions.merchant FROM transactions WHERE transactions.account_id = 3 AND transactions.account_id IN (SELECT id FROM accounts WHERE balance <= 49000)
