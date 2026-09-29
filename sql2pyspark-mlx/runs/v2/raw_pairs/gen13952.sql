select
    id,
    txn_type,
    txn_time,
    amount,
    sum(amount) over (partition by txn_type order by txn_time, id rows between unbounded preceding and current row) running_amount
from transactions
where amount between 2900 and 3100
order by txn_type;
