SELECT id, merchant, txn_type, DATEDIFF(CURRENT_DATE(), txn_time) days_since_txn_time FROM transactions WHERE HOUR(txn_time) BETWEEN 7 AND 13;
