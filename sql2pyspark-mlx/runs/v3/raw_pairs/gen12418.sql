with store_summary as (
    select store_id, avg(revenue) as avg_revenue
    from store_sales
    group by store_id
)
select
    s.id,
    s.store_name,
    s.opened_date,
    ss.avg_revenue
from stores s
left join store_summary ss on s.id = ss.store_id
