SELECT id, amount, is_flagged, DATE_FORMAT(txn_time, 'yyyy-MM') AS txn_time_label FROM transactions WHERE amount < 4000;
