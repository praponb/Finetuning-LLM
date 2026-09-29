SELECT merchant, txn_type, is_flagged, MAX(txn_time) AS last_txn_time FROM transactions GROUP BY merchant, txn_type, is_flagged
