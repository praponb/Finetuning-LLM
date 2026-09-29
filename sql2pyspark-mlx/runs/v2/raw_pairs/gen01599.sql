select
    quantity_bucket,
    order_id,
    avg_quantity,
    min_unit_price
from (
    select
        case when quantity >= 8 then 'high' when quantity >= 4 then 'mid' else 'low' end quantity_bucket,
        order_id,
        avg(quantity) avg_quantity,
        min(unit_price) min_unit_price
    from order_items
    group by case when quantity >= 8 then 'high' when quantity >= 4 then 'mid' else 'low' end, order_id
) t
where avg_quantity >= 4.9
order by order_id desc;
