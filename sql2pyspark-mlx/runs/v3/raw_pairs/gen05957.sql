select order_id, case when quantity >= 9 then 'High' when quantity >= 4 then 'Medium' else 'Low' end as quantity_tier
from order_items
order by order_id
