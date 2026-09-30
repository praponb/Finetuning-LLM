select
    quantity,
    store_id,
    round(quantity, 1) as quantity_rounded,
    case when quantity >= 251 then 'High' when quantity >= 87 then 'Medium' else 'Low' end as quantity_band
from inventory
where store_id in (4, 5, 6)
