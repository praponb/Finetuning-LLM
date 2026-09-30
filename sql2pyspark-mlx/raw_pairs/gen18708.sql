select *, if(is_flagged, 'Yes', 'No') as is_flagged_label
from transactions
order by txn_time;
