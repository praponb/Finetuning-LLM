with store_sales_by_store as (
    select store_id, avg(revenue) as avg_revenue, max(revenue) as max_revenue
    from store_sales
    group by store_id
)
select
    s.id,
    s.store_name,
    ssbs.avg_revenue,
    ssbs.max_revenue
from stores s
join store_sales_by_store ssbs on s.id = ssbs.store_id
