select txn_type, trim(txn_type) as clean_txn_type
from transactions
order by clean_txn_type
