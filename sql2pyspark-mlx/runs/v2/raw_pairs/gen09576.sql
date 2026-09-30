select id, merchant, length(txn_type) txn_type_length
from transactions
where txn_type in ('FEE', 'CREDIT')
