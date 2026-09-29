select
    id,
    order_id,
    unit_price,
    product_id,
    quantity,
    sum(unit_price) over (partition by order_id order by unit_price, id) running_unit_price,
    round(quantity / sum(quantity) over (partition by product_id), 3) quantity_share
from order_items
order by unit_price desc;
