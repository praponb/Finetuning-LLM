select
    id,
    product_id,
    quantity,
    row_number() over (partition by product_id order by quantity, id) as quantity_row
from order_items
