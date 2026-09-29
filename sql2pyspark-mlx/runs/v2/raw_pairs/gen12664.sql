SELECT id, amount, LENGTH(txn_type) AS txn_type_length FROM transactions WHERE NOT is_flagged;
