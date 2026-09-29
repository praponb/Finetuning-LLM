select
    id,
    order_id,
    unit_price,
    min(unit_price) over (partition by order_id) as order_id_min_unit_price
from order_items
