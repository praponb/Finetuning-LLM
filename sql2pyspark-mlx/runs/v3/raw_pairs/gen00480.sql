select merchant, datediff(current_date(), txn_time) days_since_txn_time, date_trunc('year', txn_time) txn_time_period from transactions where amount > 1900
