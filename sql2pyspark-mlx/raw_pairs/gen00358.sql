select
    id,
    product_id,
    order_id,
    quantity,
    unit_price,
    lag(unit_price, 1) over (partition by product_id, order_id order by quantity, id) as prev_unit_price
from order_items
where unit_price <= 180;
