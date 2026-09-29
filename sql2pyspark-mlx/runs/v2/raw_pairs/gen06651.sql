select quarter(txn_time) as txn_quarter, concat_ws(', ', sort_array(collect_set(merchant))) as merchant_values, max(amount) as max_amount
from transactions
group by quarter(txn_time);
