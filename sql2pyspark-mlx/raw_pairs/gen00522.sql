select date_trunc('MONTH', txn_time) as txn_month, min(txn_time) as min_txn_time, count(*) as cnt from transactions group by date_trunc('MONTH', txn_time) order by cnt desc
