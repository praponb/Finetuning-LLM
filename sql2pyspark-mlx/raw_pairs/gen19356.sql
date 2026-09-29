with valid_order_items as (
    select *
    from order_items
    where unit_price between 110 and 350 or unit_price is not null
),
agg as (
    select product_id, count(unit_price) as unit_price_count, count(distinct unit_price) as unique_orders
    from valid_order_items
    group by product_id
)
select *
from agg
order by unique_orders desc;
