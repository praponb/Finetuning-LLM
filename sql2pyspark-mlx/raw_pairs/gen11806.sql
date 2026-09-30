with RecentOrderItems as (
    select *
    from order_items
    where product_id is null
),
summary as (
    select product_id, min(quantity) as min_quantity
    from RecentOrderItems
    group by product_id
)
select *
from summary
where min_quantity < 3.8
order by product_id
