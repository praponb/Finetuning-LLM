select
    id,
    is_flagged,
    if(is_flagged, 'Yes', 'No') as is_flagged_label,
    concat(txn_type, ' - ', merchant) as txn_type_merchant
from transactions
where amount between 1700 and 2500 and merchant is null and merchant <> 'Lazada'
