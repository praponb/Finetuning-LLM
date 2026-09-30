select
    id,
    merchant,
    amount,
    txn_time,
    last_value(txn_time) over (partition by merchant order by amount, id rows between unbounded preceding and unbounded following) as last_txn_time
from transactions
where txn_type in ('REFUND', 'FEE')
