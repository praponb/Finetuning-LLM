select distinct merchant, txn_type
from transactions
where is_flagged = true;
