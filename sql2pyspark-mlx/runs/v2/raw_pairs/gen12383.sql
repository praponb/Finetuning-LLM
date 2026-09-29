SELECT id, merchant, txn_time, account_id FROM transactions WHERE is_flagged AND NOT is_flagged;
