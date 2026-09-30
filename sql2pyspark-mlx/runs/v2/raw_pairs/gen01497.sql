select merchant, count(distinct amount) as unique_accounts
from transactions
where txn_type in ('CREDIT', 'FEE', 'REFUND') and txn_type = 'FEE' and to_date(txn_time) >= '2024-11-01'
group by merchant
having unique_accounts >= 5
