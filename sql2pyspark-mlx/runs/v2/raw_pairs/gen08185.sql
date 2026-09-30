SELECT amount, txn_type, TO_DATE(txn_time) AS txn_time_date, MONTH(txn_time) AS txn_time_month FROM transactions
