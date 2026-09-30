select
    stores.id,
    stores.sqft,
    t.max_quantity,
    t.avg_quantity
from stores
left join (select store_id, max(quantity) max_quantity, round(avg(quantity), 2) avg_quantity
    from inventory
    group by store_id) t on stores.id = t.store_id;
