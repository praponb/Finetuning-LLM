SELECT id, txn_type, is_flagged, txn_time, HOUR(txn_time) txn_time_hour FROM transactions;
