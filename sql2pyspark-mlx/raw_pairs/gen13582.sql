SELECT id, account_id, is_flagged, COALESCE(amount, 0) amount_filled FROM transactions WHERE amount <= 1100
