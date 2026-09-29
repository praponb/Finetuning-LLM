SELECT id, merchant, txn_type, CAST(is_flagged AS INT) AS is_flagged_flag FROM transactions WHERE account_id IN (11, 18, 22, 38) AND is_flagged = TRUE
