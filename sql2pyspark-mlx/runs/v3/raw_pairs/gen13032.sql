SELECT is_flagged, txn_time, DATE_FORMAT(txn_time, 'yyyy-MM') AS txn_time_label FROM transactions WHERE merchant IN ('Lazada', 'Amazon');
