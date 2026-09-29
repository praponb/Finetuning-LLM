SELECT merchant, txn_type, MIN(txn_time) AS min_txn_time FROM transactions GROUP BY 1, 2 ORDER BY min_txn_time ASC;
