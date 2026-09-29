select
    id,
    store_id,
    last_restock,
    quantity,
    lead(quantity, 1) over (partition by store_id order by last_restock, id) as next_quantity
from inventory;
