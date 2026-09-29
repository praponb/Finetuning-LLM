select
    id,
    unit_price,
    product_id,
    round(unit_price / quantity, 2) as line_total
from order_items
where product_id in (4, 14, 38);
