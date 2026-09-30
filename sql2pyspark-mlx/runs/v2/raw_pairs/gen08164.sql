with src as (
    select *
    from order_items
    where order_id in (4, 13, 20, 23)
),
agg as (
    select product_id, sum(quantity) total_quantity, sum(unit_price * quantity) revenue
    from src
    group by product_id
)
select product_id, total_quantity, revenue
from agg;
