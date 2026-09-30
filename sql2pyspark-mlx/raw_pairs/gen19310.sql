select
    id,
    product_id,
    unit_price,
    quantity
from (
    select *, row_number() over (partition by product_id order by unit_price, id) as pos
    from order_items
) as t
where pos <= 2
order by id desc, 2 desc
