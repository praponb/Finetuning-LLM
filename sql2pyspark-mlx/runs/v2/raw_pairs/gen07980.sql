select *, if(is_flagged, 'Yes', 'No') as is_flagged_label
from transactions
where txn_time < '2025-05-01' and amount < 1100;
