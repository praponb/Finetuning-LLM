select
    id,
    account_id,
    txn_type,
    amount,
    rank() over (partition by account_id, txn_type order by amount desc) as rnk
from transactions;
