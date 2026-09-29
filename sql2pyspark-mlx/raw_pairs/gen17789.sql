select txn_type, count(distinct amount) as unique_merchants from transactions group by txn_type
