SELECT id, merchant, txn_type, account_id FROM transactions WHERE (amount >= 1500 AND account_id IN (26, 29)) OR amount >= 4000
