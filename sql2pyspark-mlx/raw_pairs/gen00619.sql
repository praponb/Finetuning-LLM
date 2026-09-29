select
    quarter(txn_time) as txn_quarter,
    is_flagged,
    round((100 * sum(case when txn_type = 'REFUND' then 1 else 0 end)) / count(*), 1) as pct_refund,
    sort_array(collect_set(merchant)) as merchant_list
from transactions
group by rollup(quarter(txn_time), is_flagged);
