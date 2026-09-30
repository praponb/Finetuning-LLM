select case when txn_type in ('FEE', 'REFUND') then 'core' else 'other' end as txn_type_group, concat_ws(', ', sort_array(collect_set(merchant))) as merchant_values
from transactions
where txn_time >= '2025-05-01'
group by 1
