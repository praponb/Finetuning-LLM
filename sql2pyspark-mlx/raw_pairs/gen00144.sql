select
    *,
    lag(quantity) over (partition by order_id, product_id order by unit_price, id) as prev_quantity,
    lag(unit_price) over (partition by order_id, product_id order by unit_price, id) as prev_unit_price,
    row_number() over (partition by order_id, product_id order by unit_price, id) as event_seq
from order_items
