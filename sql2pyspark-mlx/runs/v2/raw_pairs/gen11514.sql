select to_date(txn_time) as txn_day, sum(amount) as total_amount, sum(sum(amount)) over (order by to_date(txn_time)) as cumulative_amount
from transactions
group by to_date(txn_time)
order by cumulative_amount;
