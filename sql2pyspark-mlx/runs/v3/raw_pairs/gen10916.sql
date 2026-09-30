select
    unit_price,
    product_id,
    order_id,
    unit_price - quantity as line_total
from order_items
order by line_total desc
