select product_id, quantity_range
from (
    select product_id, max(quantity) - min(quantity) as quantity_range
    from inventory
    group by product_id
) as t
where quantity_range < 80;
