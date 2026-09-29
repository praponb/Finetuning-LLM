with base as (
    select *, greatest(quantity, unit_price) as greatest_quantity_unit_price
    from order_items
    where product_id is not null
)
select
    id,
    product_id,
    quantity,
    unit_price,
    min(quantity) over w_running as product_id_min_quantity,
    sum(unit_price) over (partition by order_id order by quantity, id rows between unbounded preceding and current row) as cumulative_unit_price
from base
window w_running as (partition by product_id)
