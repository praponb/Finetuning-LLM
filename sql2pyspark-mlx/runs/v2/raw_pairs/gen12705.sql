select
    id,
    account_id,
    txn_time,
    merchant,
    first_value(merchant) over (partition by account_id order by txn_time, id) first_merchant
from transactions
where is_flagged
