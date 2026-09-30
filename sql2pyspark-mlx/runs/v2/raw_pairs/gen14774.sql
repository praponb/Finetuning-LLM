select
    id,
    unit_price,
    quantity,
    least(quantity, unit_price) as least_quantity_unit_price
from order_items
where unit_price > 110 and unit_price >= 220;
