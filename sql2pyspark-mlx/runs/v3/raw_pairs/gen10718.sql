SELECT *, DATE_FORMAT(txn_time, 'yyyy-MM-dd') AS txn_time_label FROM transactions WHERE NOT is_flagged;
