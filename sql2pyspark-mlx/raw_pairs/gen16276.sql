select
    hour(txn_time) as txn_hour,
    round((100 * sum(case when txn_type = 'CREDIT' then 1 else 0 end)) / count(*), 1) as pct_credit,
    concat_ws(', ', sort_array(collect_set(txn_type))) as txn_type_values,
    min(txn_time) as first_txn_time
from transactions
where hour(txn_time) between 10 and 14
group by hour(txn_time)
having round((100 * sum(case when txn_type = 'CREDIT' then 1 else 0 end)) / count(*), 1) >= 60
