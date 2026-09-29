select
    id,
    product_id,
    quantity,
    unit_price * quantity line_total,
    quantity * unit_price line_total_2
from order_items
where product_id is null and (product_id in (9, 27, 36) or quantity > 3);
