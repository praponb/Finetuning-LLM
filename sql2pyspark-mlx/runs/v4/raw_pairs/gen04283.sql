select *
from transactions
where hour(txn_time) between 6 and 9;
