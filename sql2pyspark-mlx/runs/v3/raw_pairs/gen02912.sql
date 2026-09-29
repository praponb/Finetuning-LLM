select
    id,
    amount,
    txn_time,
    txn_type,
    upper(txn_type) as txn_type_upper
from transactions
where (is_flagged and txn_type = 'REFUND') or txn_type <> 'FEE'
