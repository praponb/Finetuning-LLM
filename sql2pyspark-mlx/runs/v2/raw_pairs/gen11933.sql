SELECT is_flagged, MIN(txn_time) min_txn_time FROM transactions WHERE txn_type = 'FEE' GROUP BY is_flagged ORDER BY min_txn_time;
