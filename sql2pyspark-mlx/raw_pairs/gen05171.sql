with store_stats as (
    select store_id, max(quantity) - min(quantity) as quantity_range
    from inventory
    group by store_id
)
select s.region, s.store_name, ss.quantity_range
from stores s
inner join store_stats ss on s.id = ss.store_id
where ss.quantity_range > 31
order by s.store_name
