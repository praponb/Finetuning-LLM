select
    id,
    txn_time,
    amount,
    account_id,
    lead(amount, 1, 0) over (order by txn_time, id) as next_amount,
    dense_rank() over (partition by account_id order by txn_time) as dense_rnk
from transactions
where amount between 3400 and 3600
order by id desc;
