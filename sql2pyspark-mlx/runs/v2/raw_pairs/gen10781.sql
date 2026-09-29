select hour(txn_time) txn_hour, min(txn_time) min_txn_time
from transactions
group by hour(txn_time)
