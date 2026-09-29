select
    id,
    order_id,
    quantity,
    round(quantity / sum(quantity) over (partition by order_id), 3) as quantity_share
from order_items
where product_id = 17
