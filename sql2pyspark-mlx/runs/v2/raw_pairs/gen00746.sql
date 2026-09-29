select
    id,
    txn_type,
    txn_time,
    account_id,
    last_value(account_id) over (partition by txn_type order by txn_time, id rows between unbounded preceding and unbounded following) as last_account_id
from transactions
where txn_type <> 'FEE';
