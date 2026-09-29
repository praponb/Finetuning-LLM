select
    id,
    txn_type,
    txn_time,
    if(is_flagged, 'Yes', 'No') is_flagged_label,
    if(is_flagged, 'Yes', 'No') is_flagged_label_2
from transactions
where amount > 2600;
