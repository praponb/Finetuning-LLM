with filtered_order_items as (
    select *
    from order_items
    where order_id = 39
),
OrderItemsStats as (
    select case when quantity >= 8 then 'high' when quantity >= 5 then 'mid' else 'low' end as quantity_bucket, sum(quantity * unit_price) as revenue
    from filtered_order_items
    group by case when quantity >= 8 then 'high' when quantity >= 5 then 'mid' else 'low' end
)
select quantity_bucket, revenue
from OrderItemsStats
order by revenue desc, quantity_bucket asc
limit 10;
