select
    id,
    unit_price,
    greatest(quantity, unit_price) as greatest_quantity_unit_price,
    unit_price * quantity as line_total
from order_items
where product_id is null
order by id desc
limit 10;
