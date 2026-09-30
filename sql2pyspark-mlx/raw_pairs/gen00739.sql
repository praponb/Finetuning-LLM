select
    id,
    txn_type,
    is_flagged,
    account_id,
    cast(is_flagged as int) as is_flagged_flag
from transactions
where is_flagged;
