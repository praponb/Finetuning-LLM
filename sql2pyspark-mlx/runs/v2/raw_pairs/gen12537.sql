SELECT merchant, txn_type FROM transactions WHERE amount BETWEEN 1400 AND 2500 UNION ALL SELECT merchant, txn_type FROM transactions WHERE merchant = 'Grab'
