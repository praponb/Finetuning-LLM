select
    id,
    unit_price,
    order_id,
    dense_rank() over (order by unit_price) as unit_price_dense_rank,
    lead(unit_price, 2, 0) over (partition by order_id order by unit_price, id) as next_unit_price
from order_items
where quantity > 3
