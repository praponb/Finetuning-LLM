select
    id,
    product_id,
    unit_price,
    order_id
from (
    select
        id,
        product_id,
        unit_price,
        order_id,
        row_number() over (partition by product_id order by unit_price desc, id) as pos
    from order_items
) as t
where pos <= 2
order by id desc, unit_price desc;
