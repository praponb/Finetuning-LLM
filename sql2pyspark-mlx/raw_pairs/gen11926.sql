with store_summary as (
    select store_id, avg(units) avg_units
    from store_sales
    where units is null
    group by store_id
)
select s.region, s.store_name, ss.avg_units
from stores s
left join store_summary ss on s.id = ss.store_id
