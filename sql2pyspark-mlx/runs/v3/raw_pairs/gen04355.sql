select txn_type, min(txn_time) as first_txn_time from transactions group by txn_type
