select
    id,
    txn_time,
    amount,
    lag(amount, 1) over (order by txn_time, id) as prev_amount
from transactions;
