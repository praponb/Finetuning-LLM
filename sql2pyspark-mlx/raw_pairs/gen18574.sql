SELECT merchant, account_id, txn_time FROM transactions WHERE NOT is_flagged AND txn_type != 'DEBIT' AND account_id = 32;
