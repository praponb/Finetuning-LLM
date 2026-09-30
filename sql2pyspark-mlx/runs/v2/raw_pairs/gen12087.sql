select
    id,
    product_id,
    unit_price,
    order_id,
    ceil(unit_price) as unit_price_whole,
    unit_price + quantity as line_total
from order_items
where product_id = 25 and product_id in (19, 26, 28) and product_id is null;
