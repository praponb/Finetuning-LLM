select
    merchant,
    sum(amount) as total_amount,
    count(*) as cnt,
    dense_rank() over (order by sum(amount) desc) as rnk
from transactions
where txn_type = 'REFUND'
group by merchant
