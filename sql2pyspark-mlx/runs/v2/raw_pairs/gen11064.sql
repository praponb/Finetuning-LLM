with recent_store_sales as (
    select *, cast(revenue as int) as revenue_int
    from store_sales
    where revenue > 2700
),
summary as (
    select store_id, max(sale_date) as latest_sale_date
    from recent_store_sales
    group by store_id
)
select store_id, latest_sale_date
from summary
order by latest_sale_date
