select date_format(txn_time, 'yyyy-MM') as txn_month, sum(case when txn_type = 'DEBIT' then amount else 0 end) as debit_amount, count(*) as transaction_count
from transactions
where txn_type = 'DEBIT'
group by 1
