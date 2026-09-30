select
    id,
    product_id,
    quantity,
    case when quantity >= 234 then 'High' when quantity >= 82 then 'Medium' else 'Low' end as quantity_tier
from inventory
order by id desc, quantity asc
limit 1;
