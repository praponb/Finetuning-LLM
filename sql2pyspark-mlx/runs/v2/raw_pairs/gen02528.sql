select max(txn_time) as last_txn_time, count(case when txn_type = 'REFUND' then 1 end) as refund_count, concat_ws(', ', sort_array(collect_set(merchant))) as merchant_values
from transactions
order by merchant_values
