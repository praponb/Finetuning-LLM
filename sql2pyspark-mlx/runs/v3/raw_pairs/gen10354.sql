SELECT id, amount, is_flagged, account_id, CAST(is_flagged AS INT) AS is_flagged_flag FROM transactions WHERE amount > 3000
