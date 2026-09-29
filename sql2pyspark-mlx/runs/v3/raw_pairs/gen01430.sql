SELECT account_id, txn_time, txn_type, LENGTH(txn_type) AS txn_type_length FROM transactions WHERE is_flagged = TRUE;
