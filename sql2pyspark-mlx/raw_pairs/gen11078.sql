select month(txn_time) as txn_month, sum(amount) as total_amount, sum(sum(amount)) over (order by month(txn_time)) as cumulative_amount
from transactions
group by month(txn_time)
order by total_amount desc, txn_month
limit 10
