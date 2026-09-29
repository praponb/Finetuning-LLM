SELECT id, amount, TRIM(txn_type) AS clean_txn_type FROM transactions WHERE is_flagged ORDER BY id
