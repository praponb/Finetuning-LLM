with valid_order_items as (
    select *
    from order_items
    where order_id in (3, 13, 32) and unit_price between 280 and 390
),
agg as (
    select order_id, sum(quantity * unit_price) as revenue
    from valid_order_items
    group by order_id
)
select order_id, revenue
from agg
order by order_id desc, 2 desc;
