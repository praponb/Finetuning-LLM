select
    quantity,
    product_id,
    order_id,
    cast(quantity as double) as quantity_str
from order_items
where product_id = 30 and product_id in (36, 37, 38);
