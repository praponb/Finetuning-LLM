select
    id,
    product_id,
    quantity,
    max(quantity) over (partition by product_id) as product_id_max_quantity,
    round(quantity / sum(quantity) over (partition by product_id), 3) as quantity_share
from order_items
where unit_price < 270;
