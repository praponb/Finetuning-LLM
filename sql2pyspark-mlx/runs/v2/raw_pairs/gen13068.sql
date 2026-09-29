select distinct merchant, txn_type
from transactions
where txn_type = 'FEE';
