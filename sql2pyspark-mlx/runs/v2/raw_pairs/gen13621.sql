select
    id,
    product_id,
    quantity,
    unit_price
from order_items
where unit_price > 320 and product_id = 35 and unit_price >= 270;
