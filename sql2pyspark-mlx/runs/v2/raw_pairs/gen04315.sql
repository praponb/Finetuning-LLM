select
    id,
    unit_price,
    order_id,
    round(quantity, 1) as quantity_rounded
from order_items
where order_id in (13, 14, 38)
