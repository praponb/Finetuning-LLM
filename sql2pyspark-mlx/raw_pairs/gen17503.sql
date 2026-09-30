select *, datediff(current_date(), txn_time) / 30 as months_since_txn_time, substring(txn_type, 1, 2) as txn_type_prefix from transactions order by txn_time asc
