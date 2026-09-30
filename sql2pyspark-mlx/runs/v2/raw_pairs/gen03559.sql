with RecentOrderItems as (
    select *
    from order_items
    where product_id in (18, 32, 40) or product_id is null
),
order_items_stats as (
    select product_id, order_id, count(*) as num_order_items
    from RecentOrderItems
    group by product_id, order_id
)
select product_id, order_id, num_order_items
from order_items_stats;
