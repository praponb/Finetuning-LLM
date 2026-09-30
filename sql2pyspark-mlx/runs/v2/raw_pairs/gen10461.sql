select hour(txn_time) as txn_hour, sum(amount) as total_amount, count(amount) as amount_count from transactions group by hour(txn_time) having sum(amount) < 9000
