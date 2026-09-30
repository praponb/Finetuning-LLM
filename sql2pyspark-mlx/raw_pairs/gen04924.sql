SELECT is_flagged, MIN(txn_time) min_txn_time FROM transactions WHERE account_id = 17 AND (account_id IN (5, 31, 35) OR txn_type = 'FEE') GROUP BY 1 ORDER BY min_txn_time;
