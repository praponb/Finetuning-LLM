select *, row_number() over (partition by account_id order by txn_time nulls first, id) as rn
from transactions
qualify rn = 1
