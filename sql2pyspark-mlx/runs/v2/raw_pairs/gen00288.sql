select
    txn_type,
    to_date(txn_time) txn_day,
    sum(case when merchant = 'Amazon' then amount else 0 end) amazon_amount,
    max(txn_time) last_txn_time
from transactions
where account_id in (16, 17, 26, 30)
group by txn_type, to_date(txn_time)
order by last_txn_time asc;
