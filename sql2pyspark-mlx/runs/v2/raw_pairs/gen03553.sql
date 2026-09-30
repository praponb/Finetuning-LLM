select
    hour(txn_time) as txn_hour,
    min(txn_time) as first_txn_time,
    sum(amount) as total_amount,
    max(amount) - min(amount) as amount_range
from transactions
where amount < 1600
group by hour(txn_time)
having sum(case when merchant = 'Shell' then 1 else 0 end) < 70;
