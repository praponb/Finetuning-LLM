SELECT id, txn_time, merchant, COALESCE(amount, 0) amount_filled, CAST(is_flagged AS INT) is_flagged_flag FROM transactions WHERE txn_type IN ('DEBIT', 'REFUND');
