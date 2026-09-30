select order_id, min_quantity
from (
    select order_id, min(quantity) as min_quantity
    from order_items
    group by order_id
) as agg
where min_quantity >= 4.4;
