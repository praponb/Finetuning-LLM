SELECT id, is_flagged, txn_type, LOWER(txn_type) AS txn_type_lower FROM transactions WHERE NOT is_flagged AND txn_type != 'CREDIT';
