SELECT id, txn_time, txn_type, IF(is_flagged, 'Yes', 'No') is_flagged_label, MONTH(txn_time) txn_time_month FROM transactions WHERE NOT is_flagged
