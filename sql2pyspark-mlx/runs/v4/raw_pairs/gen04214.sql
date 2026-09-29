select
    id,
    txn_type,
    txn_time,
    amount,
    sum(amount) over (partition by txn_type order by txn_time, id rows between unbounded preceding and current row) as running_amount
from transactions
