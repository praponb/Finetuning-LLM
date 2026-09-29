select distinct txn_type
from transactions
where account_id in (1, 3, 4);
