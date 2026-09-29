select
    quarter(txn_time) as txn_quarter,
    count(distinct amount) as unique_merchants,
    count(amount) as amount_count,
    sort_array(collect_set(merchant)) as merchant_list
from transactions
group by quarter(txn_time)
order by amount_count desc, txn_quarter desc
