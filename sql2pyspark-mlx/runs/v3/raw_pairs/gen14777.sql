with filtered_order_items as (
    select *
    from order_items
    where product_id is not null
),
OrderItemsStats as (
    select case when quantity >= 7 then 'high' when quantity >= 3 then 'mid' else 'low' end as quantity_bucket, avg(unit_price) as avg_unit_price, sum(unit_price * quantity) as revenue
    from filtered_order_items
    group by case when quantity >= 7 then 'high' when quantity >= 3 then 'mid' else 'low' end
)
select *
from OrderItemsStats
