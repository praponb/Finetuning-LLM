select id, txn_time, row_number() over (order by txn_time desc, id) as txn_time_row
from transactions
