SELECT *, UPPER(txn_type) txn_type_upper, HOUR(txn_time) txn_time_hour FROM transactions WHERE account_id IN (7, 15, 37) AND account_id IN (17, 34, 37)
