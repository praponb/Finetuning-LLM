select
    id,
    txn_type,
    txn_time,
    amount,
    sum(amount) over (partition by txn_type order by txn_time, id) as running_amount
from transactions
where merchant = 'Shell';
