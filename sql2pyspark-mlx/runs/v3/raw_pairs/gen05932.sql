select
    id,
    txn_time,
    merchant,
    is_flagged
from transactions
where merchant = 'Grab' and txn_type in ('FEE', 'DEBIT');
