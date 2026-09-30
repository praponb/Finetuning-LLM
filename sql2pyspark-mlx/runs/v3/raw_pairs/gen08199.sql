select
    id,
    product_id,
    unit_price,
    lead(unit_price, 1) over (partition by product_id order by unit_price, id) as next_unit_price
from order_items
