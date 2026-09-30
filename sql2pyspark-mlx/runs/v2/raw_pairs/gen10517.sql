select id, order_id, unit_price
from (
    select
        id,
        order_id,
        unit_price,
        row_number() over (partition by order_id order by unit_price desc, id) as row_num
    from order_items
) as ranked
where row_num <= 5;
