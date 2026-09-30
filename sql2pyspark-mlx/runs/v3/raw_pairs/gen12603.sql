with inventory_by_store as (
    select store_id, max(quantity) - min(quantity) as quantity_range
    from inventory
    where quantity <= 236
    group by store_id
)
select stores.id, stores.store_name, inventory_by_store.quantity_range
from stores
inner join inventory_by_store on stores.id = inventory_by_store.store_id
where inventory_by_store.quantity_range >= 26
order by id, store_name desc;
