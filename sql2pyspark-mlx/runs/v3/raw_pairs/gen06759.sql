select
    txn_type,
    merchant,
    round((100 * sum(case when merchant = '7-Eleven' then 1 else 0 end)) / count(*), 1) as pct_7_eleven,
    min(txn_time) as min_txn_time
from transactions
where txn_type in ('REFUND', 'FEE', 'DEBIT')
group by txn_type, merchant
having pct_7_eleven >= 77
order by txn_type;
