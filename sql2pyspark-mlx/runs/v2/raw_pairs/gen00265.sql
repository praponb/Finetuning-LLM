with store_summary as (
    select store_id, count(*) as inventory_count, max(quantity) - min(quantity) as quantity_range
    from inventory
    group by store_id
)
select
    s.region,
    s.sqft,
    ss.inventory_count,
    ss.quantity_range
from stores s
inner join store_summary ss on s.id = ss.store_id
order by ss.quantity_range desc;
