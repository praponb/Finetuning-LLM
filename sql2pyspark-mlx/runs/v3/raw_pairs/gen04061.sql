SELECT is_flagged, DATE_FORMAT(txn_time, 'yyyy-MM') AS txn_time_label FROM transactions WHERE amount >= 1400 OR amount > 3700 ORDER BY txn_time_label;
