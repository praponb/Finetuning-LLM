select id, merchant, length(txn_type) as txn_type_length
from transactions
where txn_time < '2025-10-01' and merchant = '7-Eleven'
