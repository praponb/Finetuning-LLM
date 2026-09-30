select
    id,
    txn_type,
    account_id,
    txn_time,
    row_number() over (partition by txn_type, account_id order by txn_time, id) txn_time_row
from transactions
