SELECT id, txn_time FROM transactions WHERE txn_type IN ('CREDIT', 'FEE') OR account_id IN (2, 10, 33, 40)
