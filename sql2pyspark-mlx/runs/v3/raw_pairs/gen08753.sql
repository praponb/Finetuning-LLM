select
    id,
    order_id,
    unit_price,
    product_id,
    sum(unit_price) over (partition by order_id) as order_id_sum_unit_price,
    sum(unit_price) over (partition by product_id) as product_id_sum_unit_price
from order_items;
