select date_format(txn_time, 'yyyy-MM') as txn_month, min(txn_time) as min_txn_time
from transactions
where to_date(txn_time) < '2025-03-01'
group by txn_month;
