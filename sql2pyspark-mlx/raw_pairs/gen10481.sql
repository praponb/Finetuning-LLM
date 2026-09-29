with filtered_order_items as (
    select *
    from order_items
    where unit_price > 110
),
order_items_stats as (
    select order_id, case when quantity >= 7 then 'high' when quantity >= 4 then 'mid' else 'low' end as quantity_bucket, max(quantity) - min(quantity) as quantity_range
    from filtered_order_items
    group by order_id, case when quantity >= 7 then 'high' when quantity >= 4 then 'mid' else 'low' end
)
select order_id, quantity_range
from order_items_stats
where quantity_range < 34
order by order_id desc
