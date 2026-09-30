with store_stats as (
    select store_id, min(revenue) as min_revenue, max(revenue) as max_revenue
    from store_sales
    group by store_id
)
select
    s.id,
    s.sqft,
    s.opened_date,
    ss.min_revenue,
    ss.max_revenue
from stores s
join store_stats ss on s.id = ss.store_id
