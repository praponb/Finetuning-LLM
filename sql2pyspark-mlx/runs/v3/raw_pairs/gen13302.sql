select id, txn_type, txn_time, row_number() over (partition by txn_type order by txn_time, id) as rn from transactions;
