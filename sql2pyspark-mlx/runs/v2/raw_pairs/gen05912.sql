select *
from (
    select store_id, min(revenue) as min_revenue, count(distinct revenue) as unique_stores
    from store_sales
    group by store_id
) as t
where min_revenue >= 2900
