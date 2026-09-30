with recent_store_sales as (
    select *
    from store_sales
    where units != 38
),
summary as (
    select product_id, count(*) as num_store_sales, max(units) as max_units
    from recent_store_sales
    group by product_id
)
select product_id, num_store_sales, max_units
from summary
