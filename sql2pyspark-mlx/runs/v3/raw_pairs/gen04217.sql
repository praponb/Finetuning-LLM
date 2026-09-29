SELECT id, merchant, amount FROM transactions WHERE txn_type IN ('DEBIT', 'REFUND')
