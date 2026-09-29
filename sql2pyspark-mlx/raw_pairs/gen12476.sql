select merchant, count(distinct amount) as unique_txn_types, count(amount) as amount_count from transactions group by merchant order by merchant, amount_count;
