select
    id,
    txn_time,
    amount,
    lead(amount, 1) over (order by txn_time, id) as next_amount
from transactions
where merchant in ('Grab', 'Amazon', '7-Eleven');
