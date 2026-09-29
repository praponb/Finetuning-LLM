select
    id,
    quantity,
    product_id,
    order_id,
    case when quantity >= 9 then 'High' when quantity >= 5 then 'Medium' else 'Low' end as quantity_level
from order_items
where product_id is not null and quantity is null
order by order_id
