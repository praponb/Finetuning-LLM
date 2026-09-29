select id, txn_type, txn_time, ntile(2) over (partition by txn_type order by txn_time desc, id) as quartile from transactions
