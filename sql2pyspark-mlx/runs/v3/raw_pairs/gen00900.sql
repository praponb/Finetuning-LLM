select
    i.id,
    i.store_id,
    i.last_restock,
    s.store_name
from inventory i
left join stores s on i.store_id = s.id
where i.quantity between 185 and 208
