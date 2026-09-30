SELECT id, txn_time, is_flagged, HOUR(txn_time) AS txn_time_hour, CONCAT(txn_type, ' - ', merchant) AS txn_type_merchant FROM transactions;
