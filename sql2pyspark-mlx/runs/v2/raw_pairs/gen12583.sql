select
    id,
    account_id,
    txn_time,
    txn_type,
    first_value(txn_type) over (partition by account_id order by txn_time, id) as first_txn_type
from transactions;
