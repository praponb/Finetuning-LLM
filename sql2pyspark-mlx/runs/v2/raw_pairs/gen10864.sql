select
    txn_type,
    sum(amount) total_amount,
    count(*) cnt,
    rank() over (order by sum(amount) desc) as amount_rank
from transactions
group by txn_type
order by total_amount desc, txn_type asc;
