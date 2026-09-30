select
    id,
    order_id,
    unit_price,
    product_id,
    quantity,
    row_number() over (partition by order_id order by unit_price, id) as rn,
    sum(unit_price) over (partition by product_id, order_id order by quantity, id rows between unbounded preceding and current row) as running_unit_price
from order_items
order by 3, product_id desc
