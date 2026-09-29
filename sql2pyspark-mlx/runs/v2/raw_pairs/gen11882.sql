select txn_type, max(txn_time) as last_txn_time, max(amount) - min(amount) as amount_range
from transactions
group by txn_type
order by 2, amount_range;
