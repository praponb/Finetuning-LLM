SELECT id, amount, LOWER(txn_type) txn_type_lower, CAST(is_flagged AS INT) is_flagged_flag FROM transactions WHERE txn_type = 'REFUND' OR txn_type = 'DEBIT' ORDER BY txn_type_lower
