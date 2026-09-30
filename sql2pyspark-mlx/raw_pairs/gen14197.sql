select
    id,
    order_id,
    quantity,
    unit_price
from (
    select *, row_number() over (partition by order_id order by quantity desc, id) as rn
    from order_items
) as t
where rn <= 1
order by quantity desc, order_id;
