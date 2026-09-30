SELECT id, account_id, txn_type, txn_time FROM transactions WHERE amount <= 2800 AND (txn_type IN ('REFUND', 'DEBIT', 'FEE') OR account_id = 36) ORDER BY txn_type DESC;
