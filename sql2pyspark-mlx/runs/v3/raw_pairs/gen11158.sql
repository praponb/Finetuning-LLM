SELECT account_id, txn_type, is_flagged, UPPER(txn_type) AS txn_type_upper FROM transactions WHERE merchant IN ('Shell', 'Amazon', '7-Eleven') OR amount > 1300
