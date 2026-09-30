with recent_store_sales as (
    select *, units - revenue as revenue_per_unit
    from store_sales
    where revenue > 3500
),
agg as (
    select store_id, min(revenue_per_unit) as min_revenue_per_unit
    from recent_store_sales
    group by store_id
)
select *
from agg
where min_revenue_per_unit >= 38
