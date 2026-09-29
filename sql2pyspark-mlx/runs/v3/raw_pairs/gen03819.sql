with inventory_by_store as (
    select store_id, count(*) as num_inventory, avg(quantity) as avg_quantity
    from inventory
    where store_id in (1, 2, 3, 4)
    group by store_id
)
select
    s.region,
    s.store_name,
    ibs.num_inventory,
    ibs.avg_quantity
from stores s
inner join inventory_by_store ibs on s.id = ibs.store_id
where ibs.avg_quantity > 240
order by num_inventory desc, avg_quantity asc;
