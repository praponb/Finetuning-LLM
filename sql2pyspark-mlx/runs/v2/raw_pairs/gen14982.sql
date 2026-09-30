with recent_order_items as (
    select *
    from order_items
    where quantity >= 3
),
summary as (
    select case when unit_price >= 370 then 'high' when unit_price >= 50 then 'mid' else 'low' end as unit_price_bucket, count(*) as num_order_items, count(distinct unit_price) as unique_products
    from recent_order_items
    group by case when unit_price >= 370 then 'high' when unit_price >= 50 then 'mid' else 'low' end
)
select *
from summary
order by unique_products, num_order_items;
