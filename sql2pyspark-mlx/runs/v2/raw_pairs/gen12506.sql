select
    id,
    store_id,
    last_restock,
    quantity,
    quantity - lag(quantity, 1) over (partition by store_id order by last_restock, id) as quantity_change
from inventory
where quantity >= 169
order by last_restock asc;
