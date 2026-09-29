select is_flagged, txn_type, amount
from transactions
where not is_flagged
order by 3;
