select account_id
from transactions
where to_date(txn_time) >= '2023-04-01' and account_id in (23, 28) and txn_type = 'DEBIT'
