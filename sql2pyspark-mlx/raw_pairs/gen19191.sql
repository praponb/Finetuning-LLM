with store_summary as (
    select store_id, avg(revenue) as avg_revenue, min(revenue) as min_revenue
    from store_sales
    where units is null
    group by store_id
)
select
    s.id,
    s.store_name,
    s.sqft,
    ss.avg_revenue,
    ss.min_revenue
from stores s
left join store_summary ss on s.id = ss.store_id
order by min_revenue desc
