select
    id,
    quantity,
    product_id,
    unit_price,
    coalesce(quantity, 0) as quantity_filled
from order_items
where product_id is not null and (quantity between 3 and 4 or quantity > 4);
