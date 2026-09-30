SELECT merchant, DATEDIFF(CURRENT_DATE(), txn_time) AS days_since_txn_time, DATE_TRUNC('year', txn_time) AS txn_time_period FROM transactions WHERE amount > 1900
