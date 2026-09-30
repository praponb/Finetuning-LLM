select
    id,
    unit_price,
    order_id,
    product_id,
    quantity * unit_price as line_total
from order_items
where quantity <= 8 and order_id in (7, 19, 21, 34)
