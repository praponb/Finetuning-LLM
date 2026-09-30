select
    quarter(txn_time) as txn_quarter,
    is_flagged,
    account_id,
    sort_array(collect_set(merchant)) as merchant_list,
    min(amount) as min_amount
from transactions
where amount <= 1000
group by quarter(txn_time), is_flagged, account_id
having min_amount < 2100
order by account_id
