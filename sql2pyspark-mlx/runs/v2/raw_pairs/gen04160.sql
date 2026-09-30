SELECT id, txn_time, is_flagged, DAYOFWEEK(txn_time) AS txn_time_dayofweek FROM transactions WHERE txn_type NOT IN ('FEE', 'CREDIT', 'DEBIT') AND is_flagged AND txn_time < '2024-09-05';
