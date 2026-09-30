select
    id,
    txn_type,
    txn_time,
    amount,
    lead(amount, 2) over (partition by txn_type order by txn_time, id) as next_amount
from transactions
where merchant != 'Amazon';
